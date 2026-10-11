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
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// hookDedupKey identifies one logical push for coalescing purposes.
//
// The key is deliberately NOT the forge's delivery GUID: the duplicate
// deliveries this exists to absorb are distinct deliveries with distinct GUIDs,
// so a GUID key would only ever catch the forge's own redeliveries — not the
// two-webhooks-for-one-push case.
//
// Including Refspec (the head:base pair) means two different PRs that happen to
// share a head commit — a stacked branch, a re-pushed cherry-pick — never
// coalesce into one another: they are genuinely different pipelines.
//
// Including Commit is what keeps this from over-deduping: two DISTINCT head
// SHAs are a real new push and must still create their own pipeline and
// supersede the previous one exactly as before (see cancelPreviousPipelines,
// which this change does not touch).
type hookDedupKey struct {
	RepoID  int64
	Refspec string
	Commit  string
}

// hookDeduper coalesces duplicate pull-request webhooks that describe the same
// push. It is a mutex-guarded in-memory TTL set: state is per-process, so a
// server restart simply empties it and degrades to the pre-existing behavior
// (which the stale-pending guard in the status poster then covers).
type hookDeduper struct {
	mu   sync.Mutex
	seen map[hookDedupKey]time.Time
	// now is injectable so tests can drive expiry deterministically instead of
	// sleeping. Nil means time.Now.
	now func() time.Time
}

func newHookDeduper() *hookDeduper {
	return &hookDeduper{seen: make(map[hookDedupKey]time.Time)}
}

func (d *hookDeduper) clock() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

// seenWithin records the key and reports whether an unexpired entry was already
// present — i.e. whether this delivery is a duplicate that should be dropped.
//
// Recording happens on both the hit and the miss path: refreshing the timestamp
// keeps a burst of N same-push deliveries collapsing to a single pipeline rather
// than one per window.
//
// Expired entries are swept opportunistically on each call. The map is keyed by
// (repo, refspec, commit) and entries live for one short window, so its
// steady-state size is bounded by the number of PRs pushed within that window —
// no background reaper needed.
func (d *hookDeduper) seenWithin(key hookDedupKey, window time.Duration) bool {
	if window <= 0 {
		return false
	}

	now := d.clock()

	d.mu.Lock()
	defer d.mu.Unlock()

	for k, at := range d.seen {
		if now.Sub(at) >= window {
			delete(d.seen, k)
		}
	}

	last, ok := d.seen[key]
	d.seen[key] = now

	return ok && now.Sub(last) < window
}

// forget drops any recorded key for a (repo, refspec) pair, regardless of head
// commit. It is how the reopen carve-out is enforced — see purgeOnClose below.
func (d *hookDeduper) forget(repoID int64, refspec string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for k := range d.seen {
		if k.RepoID == repoID && k.Refspec == refspec {
			delete(d.seen, k)
		}
	}
}

// reset drops all recorded keys. It exists for tests, which share the package
// singleton across cases and would otherwise see a previous case's keys as live
// hits on a rerun (-count>1, t.Parallel, or a CI job retry).
func (d *hookDeduper) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.seen)
}

// hookDedupWindow is the process-wide window used by PostHook. It is a package
// singleton because the webhook handler is a plain gin HandlerFunc with no
// server-scoped receiver to hang state off.
var hookDedupWindow = newHookDeduper()

// isDedupableHook reports whether a parsed webhook may participate in the dedup
// window at all.
//
// Scope is pull-request pipelines only — that is where the duplicate-delivery
// problem lives. Push pipelines keep their existing branch-keyed supersede path
// untouched, and pull_request_metadata is a different event that renders a
// different status context, so it never shares a key with a code pipeline.
//
// An EMPTY head SHA is also non-dedupable: the key is (repo, refspec, commit),
// so every SHA-less delivery would collapse onto the same key and the second
// would be silently dropped — a missing pipeline and a stranded check, the very
// wedge this exists to prevent. An absent SHA carries no dedup signal, so it
// fails open into the pre-existing create-both behavior.
func isDedupableHook(p *model.Pipeline) bool {
	return p != nil && p.Event == model.EventPull && p.Commit != ""
}

// purgeOnClose implements the "never suppress a reopened PR" carve-out.
//
// A reopen must always create its pipeline, even on an unchanged head commit: it
// is a human "run this again" signal, not a push artifact, so swallowing it
// would leave the reopened PR with no run at all.
//
// It cannot be detected by inspecting the reopen delivery itself. `opened`,
// `reopened` and `synchronize` all collapse to model.EventPull with an EMPTY
// EventReason (only the metadata actions populate a reason — see the forge
// parsers), so by the time PostHook holds the parsed pipeline a reopen is
// byte-identical to an ordinary push delivery. That indistinguishable shape is
// precisely why the wedge reproduces with two EventPull deliveries in the first
// place.
//
// So the carve-out keys off the delivery that necessarily PRECEDES a reopen: a
// PR can only be reopened if it was closed, and the close arrives as a separate
// EventPullClosed delivery on the same refspec. Clearing the window for that
// refspec on close means the following reopen always sees a cold key and
// proceeds to create its pipeline — however fast the close→reopen round trip.
//
// This is strictly more robust than an action sniff would be: it needs no forge
// parser change, works identically across every forge (all of them emit a close
// event), and fails in the safe direction — a stray purge only costs one
// non-coalesced duplicate, never a missing pipeline.
func purgeOnClose(repoID int64, p *model.Pipeline) {
	if p != nil && p.Event == model.EventPullClosed {
		hookDedupWindow.forget(repoID, p.Refspec)
	}
}
