// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package github

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The three rate-limit gauges are package-level singletons shared by every
// test in this package; resetRateLimitMetrics clears all three before the test
// runs and again on cleanup, so CollectAndCount reflects only the series the
// test under examination recorded and no test leaks state into another.
func resetRateLimitMetrics(t *testing.T) {
	t.Helper()
	reset := func() {
		rateLimitRemaining.Reset()
		rateLimitLimit.Reset()
		rateLimitReset.Reset()
	}
	reset()
	t.Cleanup(reset)
}

// stubRoundTripper is a hand-written http.RoundTripper fake: it records the
// request it saw and returns a canned (resp, err) so tests can drive the
// observer's passthrough contract without a real network.
type stubRoundTripper struct {
	resp   *http.Response
	err    error
	calls  int
	gotReq *http.Request
}

func (s *stubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.calls++
	s.gotReq = req
	return s.resp, s.err
}

// TestRateLimitObserverRoundTrip exercises the observer as it is actually wired
// into an *http.Client: a real transport reaches an httptest.Server, the
// observer records the response's X-RateLimit-* headers, and the caller's
// (resp, err) must come back untouched.
//
// Contract defended: the RoundTripper is transparent (status + body + error
// pass through, response identity preserved) AND it records the quota gauges as
// a side effect. A mutation that swallowed the error, replaced/mutated the
// response, or skipped observe() would redden this.
func TestRateLimitObserverRoundTrip(t *testing.T) {
	const resetTS int64 = 1_700_000_000

	t.Run("transparent passthrough records gauges from a live response", func(t *testing.T) {
		resetRateLimitMetrics(t)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "4999")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetTS, 10))
			w.Header().Set("X-RateLimit-Resource", "core-bdd")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("hello body"))
		}))
		defer srv.Close()

		base := &http.Transport{}
		t.Cleanup(base.CloseIdleConnections)
		observer := newRateLimitObserver(base, tokenKindUser)
		client := &http.Client{Transport: observer}

		resp, err := client.Get(srv.URL)
		require.NoError(t, err)
		defer resp.Body.Close()

		// (a) response passed through unchanged.
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "hello body", string(body))

		// (b) gauges recorded under (resource, token_kind).
		assert.Equal(t, 4999.0, testutil.ToFloat64(rateLimitRemaining.WithLabelValues("core-bdd", tokenKindUser)))
		assert.Equal(t, 5000.0, testutil.ToFloat64(rateLimitLimit.WithLabelValues("core-bdd", tokenKindUser)))
		assert.Equal(t, float64(resetTS), testutil.ToFloat64(rateLimitReset.WithLabelValues("core-bdd", tokenKindUser)))
	})

	t.Run("propagates the base transport error and records nothing on nil response", func(t *testing.T) {
		resetRateLimitMetrics(t)

		baseErr := errors.New("dial tcp: connection refused")
		stub := &stubRoundTripper{resp: nil, err: baseErr}
		observer := newRateLimitObserver(stub, tokenKindUser)

		req := httptest.NewRequest(http.MethodGet, "http://example.invalid/api", nil)
		resp, err := observer.RoundTrip(req) //nolint:bodyclose // error path: response is nil, nothing to close

		assert.Nil(t, resp)
		assert.ErrorIs(t, err, baseErr, "the base error must surface unchanged")
		assert.Equal(t, 1, stub.calls, "base transport must be called exactly once")
		// A nil response must not reach observe(); nothing recorded.
		assert.Equal(t, 0, testutil.CollectAndCount(rateLimitRemaining))
		assert.Equal(t, 0, testutil.CollectAndCount(rateLimitLimit))
		assert.Equal(t, 0, testutil.CollectAndCount(rateLimitReset))
	})

	t.Run("returns the base response by identity and still observes it", func(t *testing.T) {
		resetRateLimitMetrics(t)

		want := &http.Response{
			StatusCode: http.StatusTeapot,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("teapot")),
		}
		want.Header.Set("X-RateLimit-Limit", "60")
		want.Header.Set("X-RateLimit-Remaining", "59")
		want.Header.Set("X-RateLimit-Resource", "core-passthrough")
		stub := &stubRoundTripper{resp: want}
		observer := newRateLimitObserver(stub, tokenKindApp)

		got, err := observer.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.invalid/", nil))
		require.NoError(t, err)
		defer got.Body.Close()

		// Same pointer back: the observer must not wrap or rebuild the response.
		assert.Same(t, want, got)
		assert.Equal(t, http.StatusTeapot, got.StatusCode)
		// And observe() ran against that response's headers.
		assert.Equal(t, 59.0, testutil.ToFloat64(rateLimitRemaining.WithLabelValues("core-passthrough", tokenKindApp)))
	})
}

// TestObserveHeaderParsing covers observe()'s header-parsing branches one row
// per branch. Each row uses a resource label unique to the row and clears the
// gauges first, so CollectAndCount and the value assertions are unambiguous.
//
// Contract defended: which headers cause a record vs an early return, the
// "core" default, and that reset is set independently of remaining/limit.
func TestObserveHeaderParsing(t *testing.T) {
	tests := []struct {
		name          string
		headers       map[string]string
		tokenKind     string
		resource      string // label the record is expected under
		wantRecorded  bool   // remaining+limit series expected
		wantRemaining float64
		wantLimit     float64
		wantResetSet  bool
		wantReset     float64
	}{
		{
			name: "happy path records remaining, limit and reset",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "4999",
				"X-RateLimit-Reset":     "1700000000",
				"X-RateLimit-Resource":  "obs-happy",
			},
			tokenKind:     tokenKindUser,
			resource:      "obs-happy",
			wantRecorded:  true,
			wantRemaining: 4999,
			wantLimit:     5000,
			wantResetSet:  true,
			wantReset:     1700000000,
		},
		{
			name: "missing resource header defaults to core",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "4900",
				"X-RateLimit-Reset":     "1700000001",
			},
			tokenKind:     tokenKindUser,
			resource:      "core",
			wantRecorded:  true,
			wantRemaining: 4900,
			wantLimit:     5000,
			wantResetSet:  true,
			wantReset:     1700000001,
		},
		{
			name: "non-core resource is recorded under its own label",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "4000",
				"X-RateLimit-Reset":     "1700000002",
				"X-RateLimit-Resource":  "graphql",
			},
			tokenKind:     tokenKindApp,
			resource:      "graphql",
			wantRecorded:  true,
			wantRemaining: 4000,
			wantLimit:     5000,
			wantResetSet:  true,
			wantReset:     1700000002,
		},
		{
			name: "zero limit is recorded, distinct from a missing limit",
			headers: map[string]string{
				"X-RateLimit-Limit":     "0",
				"X-RateLimit-Remaining": "0",
				"X-RateLimit-Resource":  "obs-zero",
			},
			tokenKind:     tokenKindUser,
			resource:      "obs-zero",
			wantRecorded:  true,
			wantRemaining: 0,
			wantLimit:     0,
			wantResetSet:  false,
		},
		{
			name: "missing limit header records nothing",
			headers: map[string]string{
				"X-RateLimit-Remaining": "100",
				"X-RateLimit-Reset":     "1700000003",
				"X-RateLimit-Resource":  "obs-nolimit",
			},
			tokenKind:    tokenKindUser,
			resource:     "obs-nolimit",
			wantRecorded: false,
		},
		{
			name: "unparseable limit records nothing",
			headers: map[string]string{
				"X-RateLimit-Limit":     "abc",
				"X-RateLimit-Remaining": "100",
				"X-RateLimit-Resource":  "obs-badlimit",
			},
			tokenKind:    tokenKindUser,
			resource:     "obs-badlimit",
			wantRecorded: false,
		},
		{
			name: "unparseable remaining records nothing",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "xyz",
				"X-RateLimit-Resource":  "obs-badremaining",
			},
			tokenKind:    tokenKindUser,
			resource:     "obs-badremaining",
			wantRecorded: false,
		},
		{
			name: "missing reset sets remaining and limit but not reset",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "4000",
				"X-RateLimit-Resource":  "obs-noreset",
			},
			tokenKind:     tokenKindUser,
			resource:      "obs-noreset",
			wantRecorded:  true,
			wantRemaining: 4000,
			wantLimit:     5000,
			wantResetSet:  false,
		},
		{
			name: "unparseable reset sets remaining and limit but not reset",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "4000",
				"X-RateLimit-Reset":     "not-a-number",
				"X-RateLimit-Resource":  "obs-badreset",
			},
			tokenKind:     tokenKindUser,
			resource:      "obs-badreset",
			wantRecorded:  true,
			wantRemaining: 4000,
			wantLimit:     5000,
			wantResetSet:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetRateLimitMetrics(t)

			o := newRateLimitObserver(nil, tc.tokenKind)
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			o.observe(h)

			if !tc.wantRecorded {
				assert.Equal(t, 0, testutil.CollectAndCount(rateLimitRemaining), "remaining vec must stay empty")
				assert.Equal(t, 0, testutil.CollectAndCount(rateLimitLimit), "limit vec must stay empty")
				assert.Equal(t, 0, testutil.CollectAndCount(rateLimitReset), "reset vec must stay empty")
				return
			}

			// Count series before touching WithLabelValues (which would itself
			// create a phantom series): this proves observe() did the recording.
			assert.Equal(t, 1, testutil.CollectAndCount(rateLimitRemaining), "exactly one remaining series recorded")
			assert.Equal(t, 1, testutil.CollectAndCount(rateLimitLimit), "exactly one limit series recorded")
			if tc.wantResetSet {
				assert.Equal(t, 1, testutil.CollectAndCount(rateLimitReset), "reset series recorded")
			} else {
				assert.Equal(t, 0, testutil.CollectAndCount(rateLimitReset), "reset series NOT recorded")
			}

			assert.Equal(t, tc.wantRemaining, testutil.ToFloat64(rateLimitRemaining.WithLabelValues(tc.resource, tc.tokenKind)))
			assert.Equal(t, tc.wantLimit, testutil.ToFloat64(rateLimitLimit.WithLabelValues(tc.resource, tc.tokenKind)))
			if tc.wantResetSet {
				assert.Equal(t, tc.wantReset, testutil.ToFloat64(rateLimitReset.WithLabelValues(tc.resource, tc.tokenKind)))
			}
		})
	}
}

// TestObserveTokenKindSeparateSeries proves the app and user token kinds land on
// distinct gauge series for the same resource — the whole point of the
// token_kind label (an App-token burst must not hide a healthy user bucket, and
// vice versa).
func TestObserveTokenKindSeparateSeries(t *testing.T) {
	resetRateLimitMetrics(t)

	const resource = "obs-tokenkind"
	headers := func(remaining string) http.Header {
		h := http.Header{}
		h.Set("X-RateLimit-Limit", "5000")
		h.Set("X-RateLimit-Remaining", remaining)
		h.Set("X-RateLimit-Resource", resource)
		return h
	}

	newRateLimitObserver(nil, tokenKindApp).observe(headers("4100"))
	newRateLimitObserver(nil, tokenKindUser).observe(headers("4200"))

	assert.Equal(t, 4100.0, testutil.ToFloat64(rateLimitRemaining.WithLabelValues(resource, tokenKindApp)))
	assert.Equal(t, 4200.0, testutil.ToFloat64(rateLimitRemaining.WithLabelValues(resource, tokenKindUser)))
	// Two independent series coexist under the same resource.
	assert.Equal(t, 2, testutil.CollectAndCount(rateLimitRemaining))
}

// TestWarnLowDebounce drives the low-watermark warn through a single observer
// (the way production reuses one observer per token) and asserts the debounce
// map behaves: one warn per resource per interval, independent per resource, and
// silence above the watermark.
//
// Timing note: every observe() here runs microseconds apart — far inside
// rateLimitWarnInterval (1m) — so the debounce outcome is deterministic without
// any sleep or clock injection.
func TestWarnLowDebounce(t *testing.T) {
	resetRateLimitMetrics(t)

	// Capture zerolog output into a buffer; restore the global logger and level
	// on cleanup so this test cannot leak into others.
	var buf bytes.Buffer
	origLogger := log.Logger
	origLevel := zerolog.GlobalLevel()
	log.Logger = zerolog.New(&buf)
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	t.Cleanup(func() {
		log.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	})

	warnCount := func() int { return strings.Count(buf.String(), "nearly exhausted") }

	lowHeaders := func(resource string) http.Header {
		h := http.Header{}
		h.Set("X-RateLimit-Limit", "5000")
		h.Set("X-RateLimit-Remaining", "400") // 400/5000 = 0.08 < 0.1 watermark
		h.Set("X-RateLimit-Resource", resource)
		return h
	}

	o := newRateLimitObserver(nil, tokenKindUser)

	// Below the watermark → exactly one warn.
	o.observe(lowHeaders("warn-core"))
	assert.Equal(t, 1, warnCount(), "first low observation warns")

	// Same resource again, immediately → debounced, still one.
	o.observe(lowHeaders("warn-core"))
	assert.Equal(t, 1, warnCount(), "repeat within the interval is debounced")

	// A different resource, low at the same instant → independent debounce.
	o.observe(lowHeaders("warn-search"))
	assert.Equal(t, 2, warnCount(), "each resource debounces independently")

	// Above the watermark → never warns.
	healthy := http.Header{}
	healthy.Set("X-RateLimit-Limit", "5000")
	healthy.Set("X-RateLimit-Remaining", "4999") // 0.9998 > 0.1
	healthy.Set("X-RateLimit-Resource", "warn-healthy")
	o.observe(healthy)
	assert.Equal(t, 2, warnCount(), "a healthy bucket does not warn")

	// Exactly at the watermark → no warn: the comparison is strict `<`, so
	// 500/5000 == 0.1 must not trip. Guards against a `<=` mutation.
	boundary := http.Header{}
	boundary.Set("X-RateLimit-Limit", "5000")
	boundary.Set("X-RateLimit-Remaining", "500") // 500/5000 == 0.1, not < 0.1
	boundary.Set("X-RateLimit-Resource", "warn-boundary")
	o.observe(boundary)
	assert.Equal(t, 2, warnCount(), "remaining exactly at the watermark does not warn")
}
