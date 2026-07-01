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
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/go-github/v88/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// abuseErr builds a secondary (burst) rate-limit error that is safe to log:
// doForgeWrite calls log.Warn().Err(err) on every retry, and
// (*github.AbuseRateLimitError).Error() dereferences Response.Request, so both
// must be non-nil regardless of the active log level.
func abuseErr(retryAfter *time.Duration) *github.AbuseRateLimitError {
	return &github.AbuseRateLimitError{
		Response:   &http.Response{StatusCode: http.StatusForbidden, Request: &http.Request{}},
		RetryAfter: retryAfter,
	}
}

// TestForgeRetryWait exercises the pure wait/retriable classifier directly with
// constructed error values.
//
// Pre-fix relevance: forgeRetryWait did not exist — there was no classification
// of GitHub errors at all, so none of these mappings (least of all honoring a
// secondary-limit Retry-After) were computed anywhere.
func TestForgeRetryWait(t *testing.T) {
	const backoffArg = 250 * time.Millisecond
	retryAfter7s := 7 * time.Second

	tests := []struct {
		name          string
		err           error
		wantWait      time.Duration
		wantRetriable bool
	}{
		{
			name:          "secondary rate limit honors Retry-After",
			err:           &github.AbuseRateLimitError{RetryAfter: &retryAfter7s},
			wantWait:      7 * time.Second,
			wantRetriable: true,
		},
		{
			name:          "secondary rate limit without Retry-After falls back to backoff",
			err:           &github.AbuseRateLimitError{RetryAfter: nil},
			wantWait:      backoffArg,
			wantRetriable: true,
		},
		{
			name:          "5xx ErrorResponse backs off",
			err:           &github.ErrorResponse{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}},
			wantWait:      backoffArg,
			wantRetriable: true,
		},
		{
			name:          "404 ErrorResponse is not retriable",
			err:           &github.ErrorResponse{Response: &http.Response{StatusCode: http.StatusNotFound}},
			wantWait:      0,
			wantRetriable: false,
		},
		{
			name:          "plain error is not retriable",
			err:           errors.New("boom"),
			wantWait:      0,
			wantRetriable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wait, retriable := forgeRetryWait(tc.err, backoffArg)
			assert.Equal(t, tc.wantRetriable, retriable)
			assert.Equal(t, tc.wantWait, wait)
		})
	}

	// Primary rate limit: the wait is time.Until(Reset), so its exact value is
	// clock-sensitive. Pin the reset ~5s out and assert the wait lands in a
	// generous (0, 6s] window rather than an exact duration.
	t.Run("primary rate limit waits until reset", func(t *testing.T) {
		err := &github.RateLimitError{
			Rate: github.Rate{Reset: github.Timestamp{Time: time.Now().Add(5 * time.Second)}},
		}
		wait, retriable := forgeRetryWait(err, backoffArg)
		assert.True(t, retriable)
		assert.Positive(t, wait)
		assert.LessOrEqual(t, wait, 6*time.Second)
	})
}

// TestForgeWriteRetriesThenSucceeds proves a transient secondary rate limit is
// retried and then the write succeeds.
//
// Pre-fix relevance: there was no retry wrapper at all — a 403 secondary-limit
// error from the first call was returned straight to the caller, so fn was
// invoked exactly once and the write failed. The second, successful attempt
// asserted here simply could not happen.
func TestForgeWriteRetriesThenSucceeds(t *testing.T) {
	retryAfter := 1 * time.Millisecond
	calls := 0
	fn := func() (*github.Response, error) {
		calls++
		if calls == 1 {
			return nil, abuseErr(&retryAfter)
		}
		return &github.Response{}, nil
	}

	resp, err := doForgeWrite(context.Background(), fn)
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "fn must be retried exactly once after the rate limit")
	assert.NotNil(t, resp)
}

// TestForgeWriteGivesUpAfterMaxAttempts proves the retry loop is bounded: a
// persistently rate-limited write stops after forgeWriteMaxAttempts and surfaces
// the last error instead of hammering GitHub forever.
//
// Pre-fix relevance: with no retry loop, "max attempts" was meaningless — the
// single call's 403 was returned immediately. This asserts the new bounded-loop
// behavior (exactly 4 attempts, then give up) that did not previously exist.
func TestForgeWriteGivesUpAfterMaxAttempts(t *testing.T) {
	retryAfter := 1 * time.Millisecond
	calls := 0
	fn := func() (*github.Response, error) {
		calls++
		return nil, abuseErr(&retryAfter)
	}

	_, err := doForgeWrite(context.Background(), fn)
	require.Error(t, err)
	assert.Equal(t, forgeWriteMaxAttempts, calls, "fn must be attempted exactly forgeWriteMaxAttempts times")
	var abuse *github.AbuseRateLimitError
	assert.ErrorAs(t, err, &abuse, "the final error must be the rate-limit error")
}

// TestForgeWriteNonRetriableStopsImmediately proves a non-retriable error (a
// 404) short-circuits the loop after a single attempt.
//
// Pre-fix relevance: no wrapper existed, so every error already returned after
// one call. This guards that adding the retry loop did NOT accidentally start
// retrying errors that must not be retried — a regression the loop could
// introduce.
func TestForgeWriteNonRetriableStopsImmediately(t *testing.T) {
	calls := 0
	fn := func() (*github.Response, error) {
		calls++
		return nil, &github.ErrorResponse{
			Response: &http.Response{StatusCode: http.StatusNotFound, Request: &http.Request{}},
			Message:  "not found",
		}
	}

	_, err := doForgeWrite(context.Background(), fn)
	require.Error(t, err)
	assert.Equal(t, 1, calls, "a non-retriable error must not be retried")
}

// TestForgeWriteRespectsCtxCancel proves every wait is bounded by ctx: even when
// GitHub asks for a 10s Retry-After, doForgeWrite must not sleep past the
// caller's budget and returns ctx.Err() promptly.
//
// Pre-fix relevance: honoring Retry-After (and any bounded sleep) did not exist
// pre-fix — a 403 failed instantly, so there was nothing to bound. This asserts
// the new guarantee that a long Retry-After never blocks past ctx.
func TestForgeWriteRespectsCtxCancel(t *testing.T) {
	longRetryAfter := 10 * time.Second
	fn := func() (*github.Response, error) {
		return &github.Response{}, abuseErr(&longRetryAfter)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := doForgeWrite(ctx, fn)
		done <- err
	}()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
			"expected a context error, got %v", err)
		assert.Less(t, elapsed, 2*time.Second,
			"doForgeWrite must not sleep the full 10s Retry-After when ctx expires")
	case <-time.After(5 * time.Second):
		t.Fatal("doForgeWrite did not return; it slept past the ctx budget")
	}
}
