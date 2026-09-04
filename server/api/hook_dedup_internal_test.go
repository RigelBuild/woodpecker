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

package api

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// fakeClock drives hookDeduper's TTL deterministically. Expiry is a timing
// behavior, so it is tested with a controllable clock rather than a real sleep:
// advancing it is exact and instant, where a sleep would be both slow and
// racy under load.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestDeduper() (*hookDeduper, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	d := newHookDeduper()
	d.now = clock.now
	return d, clock
}

func testKey(commit string) hookDedupKey {
	return hookDedupKey{RepoID: 1, Refspec: "feature:main", Commit: commit}
}

// TestHookDeduperCoalescesWithinWindow is the core mechanic: the first sighting
// of a key is a miss (create the pipeline), an immediate repeat is a hit (drop
// the duplicate).
func TestHookDeduperCoalescesWithinWindow(t *testing.T) {
	d, _ := newTestDeduper()
	key := testKey("abc")

	assert.False(t, d.seenWithin(key, time.Minute), "the first delivery must be a miss and create")
	assert.True(t, d.seenWithin(key, time.Minute), "an immediate repeat must be caught as a duplicate")
}

// TestHookDeduperExpiresAfterWindow pins that the window actually closes: once
// the TTL elapses, the same key is a miss again, so a genuine re-push of the
// same commit later still gets its pipeline. A window that never expired would
// permanently blacklist a commit.
func TestHookDeduperExpiresAfterWindow(t *testing.T) {
	d, clock := newTestDeduper()
	key := testKey("abc")
	const window = 10 * time.Second

	require.False(t, d.seenWithin(key, window))
	require.True(t, d.seenWithin(key, window))

	clock.advance(window)

	assert.False(t, d.seenWithin(key, window),
		"once the window has elapsed the key must be cold again, so a later re-push still runs")
}

// TestHookDeduperCollapsesBurstRatherThanAlternating pins the refresh-on-hit
// behavior. A burst of deliveries spread across more than one window-length must
// still collapse to ONE pipeline, not one per window: each hit pushes the
// deadline out, so only the first sighting creates.
//
// Remove the unconditional `d.seen[key] = now` write and the third delivery
// below (2×6s = 12s after the first, past a 10s window measured from the first)
// would come back a miss and spawn a second pipeline.
func TestHookDeduperCollapsesBurstRatherThanAlternating(t *testing.T) {
	d, clock := newTestDeduper()
	key := testKey("abc")
	const window = 10 * time.Second

	require.False(t, d.seenWithin(key, window), "first delivery creates")

	for range 3 {
		clock.advance(6 * time.Second) // < window since the previous sighting
		assert.True(t, d.seenWithin(key, window),
			"each delivery within a window of the PREVIOUS one must stay coalesced")
	}
}

// TestHookDeduperZeroWindowNeverDedups pins the disabled default: with the
// window off the deduper is inert and never reports a hit, so behavior is
// exactly as before the feature existed.
func TestHookDeduperZeroWindowNeverDedups(t *testing.T) {
	d, _ := newTestDeduper()
	key := testKey("abc")

	assert.False(t, d.seenWithin(key, 0))
	assert.False(t, d.seenWithin(key, 0), "a disabled window must never coalesce")
}

// TestHookDeduperDistinguishesKeys pins each component of the composite key: a
// different commit, a different refspec, or a different repo is a DIFFERENT
// logical push and must not be coalesced. Drop any one field from hookDedupKey
// and the matching case here reddens.
func TestHookDeduperDistinguishesKeys(t *testing.T) {
	d, _ := newTestDeduper()
	const window = time.Minute

	base := hookDedupKey{RepoID: 1, Refspec: "feature:main", Commit: "abc"}
	require.False(t, d.seenWithin(base, window))

	differentCommit := base
	differentCommit.Commit = "def"
	assert.False(t, d.seenWithin(differentCommit, window),
		"a distinct head commit is a real new push and must not be coalesced")

	differentRefspec := base
	differentRefspec.Refspec = "other:main"
	assert.False(t, d.seenWithin(differentRefspec, window),
		"a distinct PR sharing a commit must not be coalesced")

	differentRepo := base
	differentRepo.RepoID = 2
	assert.False(t, d.seenWithin(differentRepo, window),
		"the same refspec+commit in another repo must not be coalesced")
}

// TestHookDeduperForgetClearsRefspecAcrossCommits pins the reopen carve-out's
// primitive: forget drops every key for a (repo, refspec) pair regardless of
// commit, and leaves other refspecs alone. It must clear by refspec rather than
// by exact key because the close delivery and the following reopen are matched
// on the PR, not on a particular head commit.
func TestHookDeduperForgetClearsRefspecAcrossCommits(t *testing.T) {
	d, _ := newTestDeduper()
	const window = time.Minute

	mine := testKey("abc")
	other := hookDedupKey{RepoID: 1, Refspec: "unrelated:main", Commit: "zzz"}

	require.False(t, d.seenWithin(mine, window))
	require.False(t, d.seenWithin(other, window))
	require.True(t, d.seenWithin(mine, window), "sanity: the key is recorded")

	d.forget(mine.RepoID, mine.Refspec)

	assert.False(t, d.seenWithin(mine, window),
		"forget must clear the refspec so a following reopen sees a cold key")
	assert.True(t, d.seenWithin(other, window),
		"forget must not disturb an unrelated PR's window")
}

// TestHookDeduperConcurrentAccess exercises the mutex under -race: two
// deliveries for the same key racing in from different goroutines (which is
// exactly how two near-simultaneous webhook deliveries arrive) must be safe, and
// exactly ONE of them must win the miss. That "exactly one creates" property is
// the whole point of the dedup under concurrency, not just memory safety.
func TestHookDeduperConcurrentAccess(t *testing.T) {
	d := newHookDeduper() // real clock: this is a concurrency test, not a TTL one
	key := testKey("abc")

	const goroutines = 8
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		misses int
	)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if !d.seenWithin(key, time.Minute) {
				mu.Lock()
				misses++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, misses,
		"exactly one concurrent delivery may miss (and create); the rest must be coalesced")
}

// TestIsDedupableHook pins the event scoping of the window: only pull_request
// pipelines participate. Push keeps its own branch-keyed supersede path,
// pull_request_closed is what CLEARS the window rather than consulting it, and
// pull_request_metadata renders a different status context entirely. A
// pull_request with no head SHA is also out of scope — it carries no dedup
// signal, so it must fail open and create.
func TestIsDedupableHook(t *testing.T) {
	assert.True(t, isDedupableHook(&model.Pipeline{Event: model.EventPull, Commit: "abc123"}),
		"pull_request is the event the duplicate-delivery bug lives on")

	assert.False(t, isDedupableHook(&model.Pipeline{Event: model.EventPull, Commit: ""}),
		"an EventPull with no head SHA carries no dedup signal and must fail open (create, never coalesce)")

	for _, event := range []model.WebhookEvent{
		model.EventPush,
		model.EventPullClosed,
		model.EventPullMetadata,
		model.EventTag,
		model.EventCron,
		model.EventManual,
		model.EventDeploy,
	} {
		// A head SHA is set so the EVENT is the only thing excluding these.
		assert.Falsef(t, isDedupableHook(&model.Pipeline{Event: event, Commit: "abc123"}),
			"%q must be out of scope for the pull-family dedup window", event)
	}

	assert.False(t, isDedupableHook(nil), "a nil pipeline must never be dedupable")
}
