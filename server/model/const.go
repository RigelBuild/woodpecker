// Copyright 2022 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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
	"errors"
	"fmt"
)

type WebhookEvent string //	@name	WebhookEvent

const (
	EventPush         WebhookEvent = "push"
	EventPull         WebhookEvent = "pull_request"
	EventPullClosed   WebhookEvent = "pull_request_closed"
	EventPullMetadata WebhookEvent = "pull_request_metadata"
	EventTag          WebhookEvent = "tag"
	EventRelease      WebhookEvent = "release"
	EventDeploy       WebhookEvent = "deployment"
	EventCron         WebhookEvent = "cron"
	EventManual       WebhookEvent = "manual"
)

type WebhookEventList []WebhookEvent

func (wel WebhookEventList) Len() int           { return len(wel) }
func (wel WebhookEventList) Swap(i, j int)      { wel[i], wel[j] = wel[j], wel[i] }
func (wel WebhookEventList) Less(i, j int) bool { return wel[i] < wel[j] }

var ErrInvalidWebhookEvent = errors.New("invalid webhook event")

func (s WebhookEvent) Validate() error {
	switch s {
	case EventPush, EventPull, EventPullClosed, EventPullMetadata, EventTag, EventRelease, EventDeploy, EventCron, EventManual:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrInvalidWebhookEvent, s)
	}
}

// StatusValue represent pipeline states woodpecker know.
type StatusValue string //	@name	StatusValue

const (
	StatusSkipped  StatusValue = "skipped"  // skipped as per condition of current workflow failed/success state
	StatusPending  StatusValue = "pending"  // pending to be executed
	StatusRunning  StatusValue = "running"  // currently running
	StatusSuccess  StatusValue = "success"  // successfully finished
	StatusFailure  StatusValue = "failure"  // failed to finish (exit code != 0)
	StatusKilled   StatusValue = "killed"   // killed by user
	StatusCanceled StatusValue = "canceled" // canceled but hasn't been started
	StatusError    StatusValue = "error"    // error with the config / while parsing / some other system problem
	StatusBlocked  StatusValue = "blocked"  // waiting for approval
	StatusDeclined StatusValue = "declined" // blocked and declined
	StatusCreated  StatusValue = "created"  // created / internal use only
)

var ErrInvalidStatusValue = errors.New("invalid status value")

func (s StatusValue) Validate() error {
	switch s {
	case StatusSkipped, StatusPending, StatusRunning, StatusSuccess, StatusFailure, StatusKilled, StatusCanceled, StatusError, StatusBlocked, StatusDeclined, StatusCreated:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrInvalidStatusValue, s)
	}
}

// IsTerminal reports whether a status is a FINAL state for status-reporting
// purposes — one that resolves the forge commit status rather than leaving it
// pending.
//
// The partition is deliberately the same one the GitHub mapping uses:
// convertStatus (server/forge/github/convert.go) maps exactly these states to a
// concrete GitHub commit status (failure/success) and lets every other state
// fall through to "pending". Keeping the two in lockstep is what makes the
// terminal-wins invariant expressible at the shared status poster: a
// non-terminal POST must never overwrite a terminal one for the same
// commit+context, because GitHub commit-status is last-write-wins per context.
//
// StatusSkipped is terminal in the sense that a skipped workflow never runs, but
// it is NOT terminal here: convertStatus reports it as "pending", so calling it
// terminal would let a pending-mapped write bypass the guard — precisely the
// write the guard exists to suppress. Terminality here is defined by what gets
// REPORTED, not by whether the state can still change.
func (s StatusValue) IsTerminal() bool {
	switch s {
	case StatusSuccess, StatusFailure, StatusKilled, StatusError, StatusDeclined, StatusCanceled:
		return true
	default:
		// StatusCreated, StatusPending, StatusRunning, StatusBlocked and
		// StatusSkipped (plus any future state) report as "pending".
		return false
	}
}

// RepoVisibility represent to what state a repo in woodpecker is visible to others.
type RepoVisibility string //	@name	RepoVisibility

const (
	VisibilityPublic   RepoVisibility = "public"
	VisibilityPrivate  RepoVisibility = "private"
	VisibilityInternal RepoVisibility = "internal"
)
