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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// GitHub bills API usage against several independent quota buckets (core,
// search, graphql, ...), each authenticated separately: a user OAuth token and
// a GitHub App installation token draw from *different* buckets even for the
// same endpoint. Woodpecker fans status/check-run writes onto the App token and
// UI reads (repos, permissions, config fetch) onto user tokens, so a burst on
// one can exhaust its bucket while the other looks healthy. These labels keep
// the two apart so the exhausted bucket is identifiable at a glance.
const (
	tokenKindUser = "user"
	tokenKindApp  = "app"
)

// rateLimitLowWatermark is the fraction of a bucket's limit below which the
// observer emits a warn log. Debounced per bucket by rateLimitWarnInterval so a
// sustained low window logs once, not once per request.
const (
	rateLimitLowWatermark = 0.1
	rateLimitWarnInterval = time.Minute
)

var (
	rateLimitRemaining = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "woodpecker",
		Name:      "github_ratelimit_remaining",
		Help:      "Remaining GitHub API requests in the current window, per quota resource and token kind.",
	}, []string{"resource", "token_kind"})
	rateLimitLimit = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "woodpecker",
		Name:      "github_ratelimit_limit",
		Help:      "Total GitHub API request quota for the current window, per quota resource and token kind.",
	}, []string{"resource", "token_kind"})
	rateLimitReset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "woodpecker",
		Name:      "github_ratelimit_reset_timestamp_seconds",
		Help:      "Unix time at which the GitHub API quota window resets, per quota resource and token kind.",
	}, []string{"resource", "token_kind"})
)

// rateLimitObserver is an http.RoundTripper that records GitHub's rate-limit
// response headers (X-RateLimit-*) as Prometheus gauges and warns when a bucket
// runs low. It only observes: the request and response pass through untouched,
// and a missing/blank rate-limit header (e.g. the OAuth token exchange, which
// carries none) is skipped rather than recorded as zero.
type rateLimitObserver struct {
	base      http.RoundTripper
	tokenKind string

	warnMu       sync.Mutex
	lastWarnedAt map[string]time.Time
}

func newRateLimitObserver(base http.RoundTripper, tokenKind string) *rateLimitObserver {
	return &rateLimitObserver{
		base:         base,
		tokenKind:    tokenKind,
		lastWarnedAt: make(map[string]time.Time),
	}
}

// RoundTrip implements http.RoundTripper.
func (o *rateLimitObserver) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := o.base.RoundTrip(req)
	if resp != nil {
		o.observe(resp.Header)
	}
	return resp, err
}

// observe records the rate-limit headers from a single response. GitHub always
// sends X-RateLimit-Limit/Remaining/Reset/Resource together on API responses;
// if Limit is absent the response is not a quota-billed API call and is ignored.
func (o *rateLimitObserver) observe(h http.Header) {
	limitStr := h.Get("X-RateLimit-Limit")
	if limitStr == "" {
		return
	}
	limit, err := strconv.ParseFloat(limitStr, 64)
	if err != nil {
		return
	}
	remaining, err := strconv.ParseFloat(h.Get("X-RateLimit-Remaining"), 64)
	if err != nil {
		return
	}
	// Resource names the bucket (core, search, graphql, integration_manifest,
	// ...); default to core when GitHub omits it (older/enterprise responses).
	resource := h.Get("X-RateLimit-Resource")
	if resource == "" {
		resource = "core"
	}

	rateLimitRemaining.WithLabelValues(resource, o.tokenKind).Set(remaining)
	rateLimitLimit.WithLabelValues(resource, o.tokenKind).Set(limit)
	if reset, err := strconv.ParseFloat(h.Get("X-RateLimit-Reset"), 64); err == nil {
		rateLimitReset.WithLabelValues(resource, o.tokenKind).Set(reset)
	}

	if limit > 0 && remaining/limit < rateLimitLowWatermark {
		o.warnLow(resource, remaining, limit, h.Get("X-RateLimit-Reset"))
	}
}

// warnLow logs a low-quota warning, debounced per resource so a sustained low
// window produces one line per rateLimitWarnInterval rather than per request.
func (o *rateLimitObserver) warnLow(resource string, remaining, limit float64, resetStr string) {
	o.warnMu.Lock()
	last, ok := o.lastWarnedAt[resource]
	now := time.Now()
	if ok && now.Sub(last) < rateLimitWarnInterval {
		o.warnMu.Unlock()
		return
	}
	o.lastWarnedAt[resource] = now
	o.warnMu.Unlock()

	event := log.Warn().
		Str("resource", resource).
		Str("token_kind", o.tokenKind).
		Float64("remaining", remaining).
		Float64("limit", limit)
	if reset, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
		event = event.Time("reset", time.Unix(reset, 0))
	}
	event.Msg("github API rate limit nearly exhausted")
}
