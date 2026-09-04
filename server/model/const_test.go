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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStatusValueIsTerminal pins the terminality partition for ALL eleven
// StatusValue constants. IsTerminal is the predicate the shared status poster
// uses to decide whether a POST may be suppressed as stale, so a single
// misclassified state is a live wedge (a non-terminal state wrongly called
// terminal suppresses a legitimate first "pending"; a terminal state wrongly
// called non-terminal lets a stale "pending" overwrite a resolved check).
//
// The partition MUST match what convertStatus
// (server/forge/github/convert.go:50-75) reports: everything that maps to a
// concrete GitHub status is terminal, everything that falls through to
// "pending" is not.
func TestStatusValueIsTerminal(t *testing.T) {
	terminal := []StatusValue{
		StatusSuccess,  // convertStatus -> success
		StatusFailure,  // convertStatus -> failure
		StatusKilled,   // convertStatus -> failure
		StatusError,    // convertStatus -> failure
		StatusDeclined, // convertStatus -> failure
		StatusCanceled, // convertStatus -> success (RIG-1123 supersede-before-start)
	}
	nonTerminal := []StatusValue{
		StatusCreated, // convertStatus default -> pending
		StatusPending, // convertStatus default -> pending
		StatusRunning, // convertStatus default -> pending
		StatusBlocked, // convertStatus default -> pending
		StatusSkipped, // convertStatus default -> pending (never runs, still reports pending)
	}

	// The two lists above enumerate every StatusValue constant declared in
	// const.go (11 of them) and each is asserted, so the partition is exhaustive
	// by construction. Go cannot enumerate a const group at runtime, so there is
	// no non-tautological way to auto-detect a newly-added constant here; adding
	// one requires classifying it in exactly one of these two lists.

	for _, s := range terminal {
		assert.Truef(t, s.IsTerminal(), "%q maps to a concrete GitHub status, so it must be terminal", s)
	}
	for _, s := range nonTerminal {
		assert.Falsef(t, s.IsTerminal(), "%q reports as GitHub pending, so it must NOT be terminal", s)
	}
}

// TestStatusValueIsTerminalUnknownState pins the default branch: an unrecognized
// status must be treated as NON-terminal. Terminality gates suppression of a
// status POST, so the safe default on an unknown state is to post (leaving the
// forge to see a pending) rather than to silently swallow a report.
func TestStatusValueIsTerminalUnknownState(t *testing.T) {
	assert.False(t, StatusValue("not-a-real-status").IsTerminal(),
		"an unknown status must default to non-terminal so its POST is never suppressed")
}
