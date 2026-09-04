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

package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	config_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/config/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/permissions"
	registry_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/registry/mocks"
	secret_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/secret/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

// setHookDedupWindow sets the process-global dedup window for one test and
// restores it afterwards.
func setHookDedupWindow(t *testing.T, d time.Duration) {
	t.Helper()
	orig := server.Config.Server.HookDedupWindow
	server.Config.Server.HookDedupWindow = d
	t.Cleanup(func() { server.Config.Server.HookDedupWindow = orig })
}

// hookReplay drives PostHook repeatedly against one repo and counts how many
// pipelines actually reach the store, which is the assertion RIG-1170 turns on:
// two deliveries for one push must produce ONE pipeline.
//
// It wires the same mock set as TestHook. Pipeline creation runs synchronously
// (WebhookSyncTimeout is left at its zero value, so PostHook blocks on the
// creation goroutine's completion signal rather than any timer), so by the time
// deliver returns the CreatePipeline count is final — no sleeping, no polling.
type hookReplay struct {
	t     *testing.T
	repo  *model.Repo
	user  *model.User
	token string

	mu       sync.Mutex
	created  []model.WebhookEvent
	statuses []int
}

// createdCount reports how many pipelines were created for the given event.
// Counting per-event keeps an assertion about pull_request runs from being
// muddied by an incidental pull_request_closed pipeline in the same replay.
func (h *hookReplay) createdCount(event model.WebhookEvent) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	var n int
	for _, e := range h.created {
		if e == event {
			n++
		}
	}
	return n
}

// newHookReplay seeds a repo whose ID/hash are derived from the test name, so
// concurrent or sequential tests never collide on the package-level dedup map's
// (repo, refspec, commit) keys.
func newHookReplay(t *testing.T, repoID int64) *hookReplay {
	t.Helper()
	gin.SetMode(gin.TestMode)

	server.Config.Permissions.Open = true
	server.Config.Permissions.Orgs = permissions.NewOrgs(nil)
	server.Config.Permissions.Admins = permissions.NewAdmins(nil)

	user := &model.User{ID: 123}
	hash := fmt.Sprintf("secret-%d-this-is-a-secret", repoID)
	repo := &model.Repo{
		ID:            repoID,
		ForgeRemoteID: model.ForgeRemoteID(fmt.Sprint(repoID)),
		Owner:         "owner",
		Name:          "name",
		IsActive:      true,
		AllowPull:     true,
		UserID:        user.ID,
		Hash:          hash,
	}

	repoToken := token.New(token.HookToken)
	repoToken.Set("repo-id", fmt.Sprint(repo.ID))
	signed, err := repoToken.Sign(hash)
	require.NoError(t, err)

	return &hookReplay{t: t, repo: repo, user: user, token: signed}
}

// deliver replays one webhook carrying the given parsed pipeline and records the
// HTTP status the handler produced.
func (h *hookReplay) deliver(pipeline *model.Pipeline) {
	h.t.Helper()

	_store := store_mocks.NewMockStore(h.t)
	_forge := forge_mocks.NewMockForge(h.t)
	_manager := manager_mocks.NewMockManager(h.t)
	_configService := config_service_mocks.NewMockService(h.t)
	_secretService := secret_service_mocks.NewMockService(h.t)
	_registryService := registry_service_mocks.NewMockService(h.t)

	origManager := server.Config.Services.Manager
	server.Config.Services.Manager = _manager
	defer func() { server.Config.Services.Manager = origManager }()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("store", _store)

	header := http.Header{}
	header.Set("Authorization", "Bearer "+h.token)
	c.Request = &http.Request{Header: header, URL: &url.URL{Scheme: "https"}}

	_manager.On("ForgeFromRepo", h.repo).Return(_forge, nil).Maybe()
	_forge.On("Hook", mock.Anything, mock.Anything).Return(h.repo, pipeline, nil).Maybe()
	_store.On("GetRepo", h.repo.ID).Return(h.repo, nil).Maybe()
	_store.On("GetUser", h.user.ID).Return(h.user, nil).Maybe()
	_store.On("UpdateRepo", h.repo).Return(nil).Maybe()

	// THE measurement: every pipeline that actually reaches the store, tagged with
	// the event that produced it.
	_store.On("CreatePipeline", mock.Anything).
		Run(func(args mock.Arguments) {
			p, _ := args.Get(0).(*model.Pipeline)
			h.mu.Lock()
			defer h.mu.Unlock()
			h.created = append(h.created, p.Event)
		}).
		Return(nil).Maybe()

	// Config fetch returns nothing, so creation filters out after the pipeline
	// row is written. That keeps the fixture small while still exercising the
	// real create path — and CreatePipeline, the thing being counted, has already
	// run by then.
	_manager.On("ConfigServiceFromRepo", h.repo).Return(_configService).Maybe()
	_configService.On("Fetch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	_forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{}, nil).Maybe()
	_store.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	_manager.On("SecretServiceFromRepo", h.repo).Return(_secretService).Maybe()
	_secretService.On("SecretListPipeline", mock.Anything, h.repo, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	_manager.On("RegistryServiceFromRepo", h.repo).Return(_registryService).Maybe()
	_registryService.On("RegistryListPipeline", mock.Anything, h.repo, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	_manager.On("EnvironmentService").Return(nil).Maybe()
	_store.On("DeletePipeline", mock.Anything).Return(nil).Maybe()

	api.PostHook(c)

	h.statuses = append(h.statuses, c.Writer.Status())
}

// pullHook builds the parsed pipeline shape the GitHub driver produces for an
// opened/reopened/synchronize pull_request delivery: model.EventPull with an
// EMPTY EventReason (only the metadata actions carry a reason). All three
// push-driven actions are indistinguishable here — which is exactly why the
// reopen carve-out hangs off the preceding close, not off this payload.
func pullHook(commit, refspec string) *model.Pipeline {
	return &model.Pipeline{
		Event:       model.EventPull,
		EventReason: []string{""},
		Commit:      commit,
		Refspec:     refspec,
		Ref:         "refs/pull/7/head",
		Branch:      "main",
	}
}

// TestPostHookCoalescesDoubleSameCommitDelivery is the RIG-1170 regression, and
// the primary acceptance case: one push, one pipeline.
//
// A Graphite `gt submit` force-push makes GitHub emit TWO pull_request
// deliveries ~1s apart for the same head commit (distinct delivery GUIDs, both
// model.EventPull). Pre-fix each spawns a pipeline; the pair then mutually
// cancels and races their status posts, and since commit-status is
// last-write-wins per context, a creation-time pending lands after the terminal
// status and wedges the required CI (pr) check forever.
//
// Red pre-fix: no dedup window exists, so both deliveries create — the assertion
// fails with "expected: 1 / actual: 2" ("one push must create exactly ONE
// pipeline").
func TestPostHookCoalescesDoubleSameCommitDelivery(t *testing.T) {
	setHookDedupWindow(t, 10*time.Second)

	h := newHookReplay(t, 9001)
	const commit = "8169a454deadbeefcafebabe0123456789abcdef"

	// Both deliveries describe the same push, same head commit.
	h.deliver(pullHook(commit, "feature:main"))
	h.deliver(pullHook(commit, "feature:main"))

	assert.Equal(t, 1, h.createdCount(model.EventPull),
		"two same-commit pull_request deliveries are one push and must create exactly ONE pipeline")
	assert.Equal(t, http.StatusOK, h.statuses[1],
		"the coalesced duplicate must be acknowledged 200, like the other ignored-hook paths")
}

// TestPostHookDoesNotCoalesceDistinctCommits is the never-over-dedup guarantee
// (acceptance 3). Two DISTINCT head commits are a real new push: each must still
// create its own pipeline so the existing supersede path
// (cancelPreviousPipelines, untouched by this change) can cancel the older run.
//
// Widen the key by dropping Commit and this reddens immediately: the second push
// would be swallowed and its pipeline never created.
func TestPostHookDoesNotCoalesceDistinctCommits(t *testing.T) {
	setHookDedupWindow(t, 10*time.Second)

	h := newHookReplay(t, 9002)

	h.deliver(pullHook("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "feature:main"))
	h.deliver(pullHook("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "feature:main"))

	assert.Equal(t, 2, h.createdCount(model.EventPull),
		"two distinct head commits are two real pushes and must both create a pipeline")
}

// TestPostHookDoesNotCoalesceDistinctPullRequests pins the refspec component of
// the key. Two different PRs can legitimately share a head commit (a stacked
// branch, a cherry-pick re-pushed onto another base). They render separate
// checks and must never collapse into one another.
//
// Drop Refspec from the key and this reddens: the second PR gets no pipeline.
func TestPostHookDoesNotCoalesceDistinctPullRequests(t *testing.T) {
	setHookDedupWindow(t, 10*time.Second)

	h := newHookReplay(t, 9003)
	const shared = "cccccccccccccccccccccccccccccccccccccccc"

	h.deliver(pullHook(shared, "feature-a:main"))
	h.deliver(pullHook(shared, "feature-b:main"))

	assert.Equal(t, 2, h.createdCount(model.EventPull),
		"two different PRs sharing a head commit must each get their own pipeline")
}

// TestPostHookNeverCoalescesReopen is acceptance case 2, the load-bearing
// carve-out: a close→reopen on an UNCHANGED head commit must still create a
// pipeline. Reopening is a human "run this again" signal, not a push artifact,
// so swallowing it would leave the reopened PR with no run at all and nothing to
// clear its required check.
//
// The reopen delivery is byte-identical to the original open at this seam (both
// model.EventPull, both with an empty EventReason), so the carve-out keys off the
// close that necessarily precedes it: the close purges the window for that
// refspec, leaving the reopen a cold key.
//
// Red check: remove the purgeOnClose call from PostHook and this fails with
// "expected: 2 / actual: 1" — the reopen is swallowed as a same-commit duplicate.
func TestPostHookNeverCoalescesReopen(t *testing.T) {
	setHookDedupWindow(t, 10*time.Second)

	h := newHookReplay(t, 9004)
	const commit = "dddddddddddddddddddddddddddddddddddddddd"

	// PR opened -> pipeline 1, and the key is recorded.
	h.deliver(pullHook(commit, "feature:main"))
	// PR closed: a different event, creates nothing here, but clears the window.
	h.deliver(&model.Pipeline{
		Event:   model.EventPullClosed,
		Commit:  commit,
		Refspec: "feature:main",
		Ref:     "refs/pull/7/head",
	})
	// PR reopened on the SAME commit, well inside the window -> must still run.
	h.deliver(pullHook(commit, "feature:main"))

	assert.Equal(t, 2, h.createdCount(model.EventPull),
		"a reopen on an unchanged head commit must still create its pipeline, never be coalesced")
}

// TestPostHookDedupDisabledByDefault pins the flag default. The window is opt-in
// (0 = off) for upstream-compatible behavior; with it disabled the duplicate is
// NOT coalesced and both deliveries create, exactly as before this change.
//
// This keeps the coalescing assertions honest: they prove the flag drives the
// behavior, not that deduping happens unconditionally.
func TestPostHookDedupDisabledByDefault(t *testing.T) {
	setHookDedupWindow(t, 0)

	h := newHookReplay(t, 9005)
	const commit = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	h.deliver(pullHook(commit, "feature:main"))
	h.deliver(pullHook(commit, "feature:main"))

	assert.Equal(t, 2, h.createdCount(model.EventPull),
		"with the window disabled (the default) behavior is unchanged: both deliveries create")
}

// TestPostHookDedupIgnoresPushEvents pins the event scoping. Push pipelines keep
// their existing branch-keyed supersede path and must never be touched by the
// pull-family dedup window — two push deliveries on one commit still create two
// pipelines here.
func TestPostHookDedupIgnoresPushEvents(t *testing.T) {
	setHookDedupWindow(t, 10*time.Second)

	h := newHookReplay(t, 9006)
	push := func() *model.Pipeline {
		return &model.Pipeline{
			Event:   model.EventPush,
			Commit:  "ffffffffffffffffffffffffffffffffffffffff",
			Branch:  "main",
			Refspec: "main:main",
		}
	}

	h.deliver(push())
	h.deliver(push())

	assert.Equal(t, 2, h.createdCount(model.EventPush),
		"push pipelines are out of scope for the pull-family dedup window")
}
