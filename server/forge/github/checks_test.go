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
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/go-github/v90/github"
	github_mock "github.com/migueleliasweb/go-github-mock/src/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestCheckRunStatus(t *testing.T) {
	tests := []struct {
		from model.StatusValue
		want string
	}{
		{model.StatusPending, checkRunStatusQueued},
		{model.StatusBlocked, checkRunStatusQueued},
		{model.StatusCreated, checkRunStatusQueued},
		{model.StatusRunning, checkRunStatusInProgress},
		{model.StatusSuccess, checkRunStatusCompleted},
		{model.StatusFailure, checkRunStatusCompleted},
		{model.StatusError, checkRunStatusCompleted},
		{model.StatusKilled, checkRunStatusCompleted},
		{model.StatusCanceled, checkRunStatusCompleted},
		{model.StatusDeclined, checkRunStatusCompleted},
		{model.StatusSkipped, checkRunStatusCompleted},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, checkRunStatus(tt.from), "status %q", tt.from)
	}
}

func TestCheckRunConclusion(t *testing.T) {
	tests := []struct {
		from model.StatusValue
		want string
	}{
		{model.StatusSuccess, "success"},
		{model.StatusFailure, "failure"},
		{model.StatusError, "failure"},
		{model.StatusKilled, checkRunConclusionCancelled},
		{model.StatusCanceled, checkRunConclusionCancelled},
		{model.StatusDeclined, checkRunConclusionCancelled},
		{model.StatusSkipped, "skipped"},
		// not-yet-completed states have no conclusion
		{model.StatusPending, ""},
		{model.StatusRunning, ""},
		{model.StatusBlocked, ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, checkRunConclusion(tt.from), "status %q", tt.from)
	}
}

// TestStatusChecksAPI is the behavioral test: with a GitHub App configured,
// Status reports the workflow as a check-run (not a commit status), with the
// conclusion mapped from the workflow state and the workflow ID as ExternalID.
func TestStatusChecksAPI(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	origFormat := server.Config.Server.StatusContextFormat
	origCtx := server.Config.Server.StatusContext
	server.Config.Server.StatusContext = "ci"
	server.Config.Server.StatusContextFormat = "{{ .context }}/{{ .workflow }}"
	defer func() {
		server.Config.Server.StatusContextFormat = origFormat
		server.Config.Server.StatusContext = origCtx
	}()

	var created github.CreateCheckRunOptions
	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatch(
			github_mock.GetReposInstallationByOwnerByRepo,
			github.Installation{ID: github.Ptr(int64(99))},
		),
		github_mock.WithRequestMatch(
			github_mock.PostAppInstallationsAccessTokensByInstallationId,
			github.InstallationToken{Token: github.Ptr("inst-token")},
		),
		github_mock.WithRequestMatch(
			github_mock.GetReposCommitsCheckRunsByOwnerByRepoByRef,
			github.ListCheckRunsResults{Total: github.Ptr(0), CheckRuns: []*github.CheckRun{}},
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposCheckRunsByOwnerByRepo,
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&created)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL, appID: 123, appKey: key}
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	workflow := &model.Workflow{ID: 7, Name: "lint", State: model.StatusSuccess}

	err = c.Status(ctx, &model.User{AccessToken: "x"}, repo, pipeline, workflow)
	require.NoError(t, err)

	assert.Equal(t, "ci/lint", created.Name)
	assert.Equal(t, "abc123", created.HeadSHA)
	require.NotNil(t, created.Status)
	assert.Equal(t, checkRunStatusCompleted, *created.Status)
	require.NotNil(t, created.Conclusion)
	assert.Equal(t, "success", *created.Conclusion)
	require.NotNil(t, created.ExternalID)
	assert.Equal(t, "7", *created.ExternalID)
}

// TestStatusCommitStatusFallback verifies that without an App configured the
// driver keeps using the legacy commit-status API.
func TestStatusCommitStatusFallback(t *testing.T) {
	var statusPosted bool
	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposStatusesByOwnerByRepoBySha,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				statusPosted = true
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL} // no app configured
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	workflow := &model.Workflow{ID: 7, Name: "lint", State: model.StatusSuccess}

	err = c.Status(ctx, &model.User{AccessToken: "x"}, repo, pipeline, workflow)
	require.NoError(t, err)
	assert.True(t, statusPosted, "commit status should be posted when no app is configured")
}

// setCheckRunStatusContext configures the status-context template used to render
// the check-run name, restoring the previous values when the test finishes.
func setCheckRunStatusContext(t *testing.T) {
	t.Helper()
	origFormat := server.Config.Server.StatusContextFormat
	origCtx := server.Config.Server.StatusContext
	server.Config.Server.StatusContext = "ci"
	server.Config.Server.StatusContextFormat = "{{ .context }}/{{ .workflow }}"
	t.Cleanup(func() {
		server.Config.Server.StatusContextFormat = origFormat
		server.Config.Server.StatusContext = origCtx
	})
}

// checkRunAppTokenMocks mocks the App installation-token exchange that precedes
// every Checks-API call. The installation token carries no expiry in these
// fixtures, so it is re-minted on each Status call; using handlers (not the
// panicking FIFO WithRequestMatch) lets both endpoints answer every time.
func checkRunAppTokenMocks() []github_mock.MockBackendOption {
	return []github_mock.MockBackendOption{
		github_mock.WithRequestMatchHandler(
			github_mock.GetReposInstallationByOwnerByRepo,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"id":99}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PostAppInstallationsAccessTokensByInstallationId,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"token":"inst-token"}`))
			}),
		),
	}
}

// TestCheckRunCacheAvoidsRelist pins the core anti-amplification contract:
// reporting the same workflow twice must paginate ListCheckRunsForRef only once
// (on the first, cache-miss call). The second report resolves the run from the
// in-memory cache and PATCHes it directly.
//
// Red against the pre-fix code: without the cache, createOrUpdateCheckRun listed
// on every transition, so the second report would list again and listCount
// would be 2.
func TestCheckRunCacheAvoidsRelist(t *testing.T) {
	setCheckRunStatusContext(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var listCount, createCount, updateCount int
	opts := append(
		checkRunAppTokenMocks(),
		github_mock.WithRequestMatchHandler(
			github_mock.GetReposCommitsCheckRunsByOwnerByRepoByRef,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				listCount++
				_, _ = w.Write([]byte(`{"total_count":0,"check_runs":[]}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposCheckRunsByOwnerByRepo,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				createCount++
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PatchReposCheckRunsByOwnerByRepoByCheckRunId,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				updateCount++
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(github_mock.NewMockedHTTPClient(opts...)))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL, appID: 123, appKey: key}
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	user := &model.User{AccessToken: "x"}

	// First report: cache miss → list once, then create + cache.
	require.NoError(t, c.Status(ctx, user, repo, pipeline,
		&model.Workflow{ID: 7, Name: "lint", State: model.StatusRunning}))
	// Second report of the same workflow: cache hit → PATCH directly, no relist.
	require.NoError(t, c.Status(ctx, user, repo, pipeline,
		&model.Workflow{ID: 7, Name: "lint", State: model.StatusSuccess}))

	assert.Equal(t, 1, listCount, "list must happen only on the first (cache-miss) call")
	assert.Equal(t, 1, createCount, "create must happen exactly once")
	assert.Equal(t, 1, updateCount, "the second report must update the cached run")
}

// TestCheckRunNoDowngradeAfterComplete pins the state-precedence guard: once a
// check-run is reported completed, a later out-of-order non-terminal status must
// not downgrade it. The guard returns before any GitHub API call.
//
// Red against the pre-fix code: without precedence handling the second report
// would find the run and PATCH it back to in_progress, so updateCount would be 1.
func TestCheckRunNoDowngradeAfterComplete(t *testing.T) {
	setCheckRunStatusContext(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var listCount, createCount, updateCount int
	opts := append(
		checkRunAppTokenMocks(),
		github_mock.WithRequestMatchHandler(
			github_mock.GetReposCommitsCheckRunsByOwnerByRepoByRef,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				listCount++
				_, _ = w.Write([]byte(`{"total_count":0,"check_runs":[]}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposCheckRunsByOwnerByRepo,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				createCount++
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PatchReposCheckRunsByOwnerByRepoByCheckRunId,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				updateCount++
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(github_mock.NewMockedHTTPClient(opts...)))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL, appID: 123, appKey: key}
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	user := &model.User{AccessToken: "x"}

	// First report: success → completed, creates + caches the terminal state.
	require.NoError(t, c.Status(ctx, user, repo, pipeline,
		&model.Workflow{ID: 7, Name: "lint", State: model.StatusSuccess}))
	// Second report: a late/out-of-order running update must NOT downgrade it.
	require.NoError(t, c.Status(ctx, user, repo, pipeline,
		&model.Workflow{ID: 7, Name: "lint", State: model.StatusRunning}))

	assert.Equal(t, 1, listCount, "only the first call lists; the guard short-circuits the second")
	assert.Equal(t, 1, createCount, "create must happen once")
	assert.Equal(t, 0, updateCount, "a completed run must never be downgraded to in_progress")
}

// TestCheckRunStaleIDRecreates pins the stale-ID fallback: if the cached run ID
// no longer exists, the PATCH returns 404, the cache entry is dropped, and the
// report recreates the run via CreateCheckRun.
//
// Red against the pre-fix code: with no cache there was no cached ID to go stale
// and no 404 recovery path, so this create-after-404 behavior did not exist.
func TestCheckRunStaleIDRecreates(t *testing.T) {
	setCheckRunStatusContext(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var listCount, createCount, updateCount int
	opts := append(
		checkRunAppTokenMocks(),
		github_mock.WithRequestMatchHandler(
			github_mock.GetReposCommitsCheckRunsByOwnerByRepoByRef,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				listCount++
				_, _ = w.Write([]byte(`{"total_count":0,"check_runs":[]}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposCheckRunsByOwnerByRepo,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				createCount++
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PatchReposCheckRunsByOwnerByRepoByCheckRunId,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				updateCount++
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"Not Found"}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(github_mock.NewMockedHTTPClient(opts...)))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL, appID: 123, appKey: key}
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	user := &model.User{AccessToken: "x"}

	// First report: cache miss → create run id 1 (cached).
	require.NoError(t, c.Status(ctx, user, repo, pipeline,
		&model.Workflow{ID: 7, Name: "lint", State: model.StatusRunning}))
	// Second report: cache hit → PATCH id 1 → 404 → drop cache → recreate.
	require.NoError(t, c.Status(ctx, user, repo, pipeline,
		&model.Workflow{ID: 7, Name: "lint", State: model.StatusSuccess}))

	assert.Equal(t, 1, updateCount, "the stale cached ID must be PATCHed once (the 404'd attempt)")
	assert.Equal(t, 2, createCount, "the 404 must trigger a second create to recover")
}

// TestStatusChecksAPISkipped verifies that when ReportSkippedToForge is ON, a
// skipped workflow IS reported as a check-run with the `skipped` conclusion (the
// opt-in forge path through the Checks API). The complementary flag-OFF default —
// where the same skipped workflow is dropped before any forge write — is covered
// by TestStatusSkippedGateDropsSkippedWhenReportingDisabled in
// status_skipped_test.go.
func TestStatusChecksAPISkipped(t *testing.T) {
	setReportSkippedToForge(t, true)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var created github.CreateCheckRunOptions
	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatch(
			github_mock.GetReposInstallationByOwnerByRepo,
			github.Installation{ID: github.Ptr(int64(99))},
		),
		github_mock.WithRequestMatch(
			github_mock.PostAppInstallationsAccessTokensByInstallationId,
			github.InstallationToken{Token: github.Ptr("inst-token")},
		),
		github_mock.WithRequestMatch(
			github_mock.GetReposCommitsCheckRunsByOwnerByRepoByRef,
			github.ListCheckRunsResults{Total: github.Ptr(0), CheckRuns: []*github.CheckRun{}},
		),
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposCheckRunsByOwnerByRepo,
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&created)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL, appID: 123, appKey: key}
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	workflow := &model.Workflow{ID: 7, Name: "lint", State: model.StatusSkipped}

	err = c.Status(ctx, &model.User{AccessToken: "x"}, repo, pipeline, workflow)
	require.NoError(t, err)
	require.NotNil(t, created.Status)
	assert.Equal(t, checkRunStatusCompleted, *created.Status)
	require.NotNil(t, created.Conclusion)
	assert.Equal(t, checkRunConclusionSkipped, *created.Conclusion)
}

// TestStatusSkippedNoCommitStatus verifies skipped workflows are not reported
// via the commit-status API (which has no skipped state) when no App is set.
func TestStatusSkippedNoCommitStatus(t *testing.T) {
	var statusPosted bool
	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposStatusesByOwnerByRepoBySha,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				statusPosted = true
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL} // no app configured
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	workflow := &model.Workflow{ID: 7, Name: "lint", State: model.StatusSkipped}

	err = c.Status(ctx, &model.User{AccessToken: "x"}, repo, pipeline, workflow)
	require.NoError(t, err)
	assert.False(t, statusPosted, "skipped workflows must not be reported via commit status")
}
