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
	"time"

	"github.com/google/go-github/v88/github"
	"github.com/rs/zerolog/log"
)

const (
	// Retry budget for a rate-limited or transient forge write: attempts are
	// capped by forgeWriteMaxAttempts and the caller treats the final error as
	// non-fatal (a failed status report never fails a pipeline).
	forgeWriteMaxAttempts = 4
	forgeWriteBaseBackoff = 500 * time.Millisecond
	forgeWriteMaxBackoff  = 8 * time.Second
)

// doForgeWrite runs a GitHub write (a status or check-run POST/PATCH) with
// bounded resilience against GitHub's secondary (burst) rate limit. GitHub trips
// that limit on bursts of concurrent writes to the same endpoint and returns a
// 403/429 with a Retry-After; hammering it prolongs the block. The helper
// honors Retry-After, applies exponential backoff on transient 5xx, and gives up
// on any other error. Every wait is bounded by ctx, so honoring a long
// Retry-After never blocks past the caller's budget (status reporting runs on a
// detached, background context so this cannot stall an agent RPC).
func doForgeWrite(ctx context.Context, fn func() (*github.Response, error)) (*github.Response, error) {
	backoff := forgeWriteBaseBackoff
	var resp *github.Response
	var err error
	for attempt := 1; ; attempt++ {
		resp, err = fn()
		if err == nil {
			return resp, nil
		}
		wait, retriable := forgeRetryWait(err, backoff)
		if !retriable || attempt >= forgeWriteMaxAttempts {
			return resp, err
		}
		log.Warn().Err(err).Dur("wait", wait).Int("attempt", attempt).
			Msg("github write rate-limited or transient; backing off")
		if !sleepCtx(ctx, wait) {
			return resp, ctx.Err()
		}
		if backoff *= 2; backoff > forgeWriteMaxBackoff {
			backoff = forgeWriteMaxBackoff
		}
	}
}

// forgeRetryWait reports how long to wait before retrying err, and whether it is
// worth retrying at all.
func forgeRetryWait(err error, backoff time.Duration) (wait time.Duration, retriable bool) {
	// Secondary (burst) rate limit: the 403 GitHub returns with Retry-After.
	var abuse *github.AbuseRateLimitError
	if errors.As(err, &abuse) {
		if abuse.RetryAfter != nil && *abuse.RetryAfter > 0 {
			return *abuse.RetryAfter, true
		}
		return backoff, true
	}
	// Primary rate limit: wait until the window resets.
	var rl *github.RateLimitError
	if errors.As(err, &rl) {
		if d := time.Until(rl.Rate.Reset.Time); d > 0 {
			return d, true
		}
		return backoff, true
	}
	// Transient server-side failures: plain backoff.
	var resp *github.ErrorResponse
	if errors.As(err, &resp) && resp.Response != nil && resp.Response.StatusCode >= 500 {
		return backoff, true
	}
	return 0, false
}

// sleepCtx waits for d or until ctx is done. It returns false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
