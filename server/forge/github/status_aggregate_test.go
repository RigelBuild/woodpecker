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
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/google/go-github/v90/github"
	github_mock "github.com/migueleliasweb/go-github-mock/src/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// TestStatusAggregate verifies the pipeline-level rollup is reported as a single
// commit status with a stable, fan-out-independent context (no `.workflow`).
func TestStatusAggregate(t *testing.T) {
	origCtx := server.Config.Server.StatusContext
	origFormat := server.Config.Server.StatusAggregateFormat
	server.Config.Server.StatusContext = "CI"
	server.Config.Server.StatusAggregateFormat = "{{ .context }} ({{ .event }})"
	defer func() {
		server.Config.Server.StatusContext = origCtx
		server.Config.Server.StatusAggregateFormat = origFormat
	}()

	var posted github.RepoStatus
	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposStatusesByOwnerByRepoBySha,
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&posted)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL}
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}

	err = c.StatusAggregate(ctx, &model.User{AccessToken: "x"}, repo, pipeline)
	require.NoError(t, err)
	assert.Equal(t, "CI (pr)", posted.GetContext())
	assert.Equal(t, statusSuccess, posted.GetState())
}

// statusAggregateFixture wires the shared harness the regression tests below
// reuse: a mocked commit-status POST endpoint whose handler is driven by
// `handler`, the mock client injected via githubClientKey, and a success
// pull-request pipeline. Returns the client, ctx, repo, user, and pipeline so
// each test can call StatusAggregate the way a real report does.
func statusAggregateFixture(t *testing.T, handler http.HandlerFunc) (*client, context.Context, *model.Repo, *model.User, *model.Pipeline) {
	t.Helper()

	origCtx := server.Config.Server.StatusContext
	origFormat := server.Config.Server.StatusAggregateFormat
	server.Config.Server.StatusContext = "CI"
	server.Config.Server.StatusAggregateFormat = "{{ .context }} ({{ .event }})"
	t.Cleanup(func() {
		server.Config.Server.StatusContext = origCtx
		server.Config.Server.StatusAggregateFormat = origFormat
	})

	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposStatusesByOwnerByRepoBySha,
			handler,
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL}
	repo := &model.Repo{Owner: "o", Name: "r"}
	user := &model.User{AccessToken: "x"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}
	return c, ctx, repo, user, pipeline
}

// TestStatusAggregateRetriesTransientThenSucceeds is the core regression for the
// production bug: a transient failure on the aggregate commit-status POST must be
// retried, not returned raw. It returns HTTP 500 on the first POST and 201 on the
// second; the fix wraps CreateStatus in doForgeWrite, so StatusAggregate must
// retry and ultimately succeed, calling the endpoint exactly twice.
//
// A 500 (transient 5xx) is used rather than a 403 secondary-rate-limit because
// go-github keeps no client-side state for a 500, so each doForgeWrite attempt is
// exactly one network call to the handler — clean, unambiguous attempt counting.
// (A 403 secondary limit sets the client's secondaryRateLimitReset and would
// short-circuit the next request before it reached the handler.)
//
// Pre-fix relevance: the aggregate path called CreateStatus raw, so the first
// 500 was returned to the caller after ONE POST and the required check stuck
// "pending". This test fails against that code (calls == 1, err != nil).
func TestStatusAggregateRetriesTransientThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	c, ctx, repo, user, pipeline := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"server error"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})

	err := c.StatusAggregate(ctx, user, repo, pipeline)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(), "the transient 500 must be retried exactly once, then succeed")
}

// TestStatusAggregateGivesUpAfterMaxAttempts proves the retry loop is bounded:
// when every aggregate-status POST fails transiently, StatusAggregate stops after
// forgeWriteMaxAttempts and surfaces the error rather than looping forever (which
// would hang the report and leave the check pending).
//
// This exhausts all 4 attempts, so it pays the real backoff of ~0.5s + 1s + 2s
// (base 500ms, doubling) ≈ 3.5s of wall time — acceptable and well under the 30s
// statusReportTimeout budget. It performs no artificial sleep of its own.
//
// Pre-fix relevance: the raw path had no loop, so "max attempts" was meaningless
// — the first 500 returned after ONE POST. This asserts the new bounded-loop
// behavior (exactly forgeWriteMaxAttempts POSTs, then give up).
func TestStatusAggregateGivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	c, ctx, repo, user, pipeline := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"server error"}`))
	})

	err := c.StatusAggregate(ctx, user, repo, pipeline)
	require.Error(t, err)
	assert.Equal(t, int32(forgeWriteMaxAttempts), calls.Load(),
		"a persistently failing POST must be attempted exactly forgeWriteMaxAttempts times, then give up")
}

// TestStatusAggregateRunsOnDetachedBudgetWhenCallerCanceled proves the decouple
// guarantee: even when the caller's ctx is already canceled (e.g. the agent gRPC
// that triggered the report has ended), StatusAggregate must STILL post the
// status, because it runs the write on context.WithoutCancel + its own timeout.
// The required branch-protection check must not be skipped just because the RPC
// that triggered it finished.
//
// Note: context.WithoutCancel preserves the context's VALUES (so the injected mock
// client still resolves via githubClientKey) while dropping the cancellation, so
// this is exercisable at this layer.
//
// Pre-fix relevance: the raw path threaded the caller's ctx straight into
// CreateStatus, so a canceled caller aborted the POST ("context canceled") and
// the report was silently skipped. This fails against that code (no POST, error).
func TestStatusAggregateRunsOnDetachedBudgetWhenCallerCanceled(t *testing.T) {
	var calls atomic.Int32
	c, ctx, repo, user, pipeline := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})

	ctx, cancel := context.WithTimeout(ctx, 0)
	cancel() // caller's context is dead before the report even starts

	err := c.StatusAggregate(ctx, user, repo, pipeline)
	require.NoError(t, err, "the report must run on its own detached budget, not the canceled caller ctx")
	assert.Equal(t, int32(1), calls.Load(), "the status must still be posted despite the canceled caller ctx")
}

// TestStatusAggregateDeployEventIsNoOp guards the early return for deployments:
// they report their own deployment status, so StatusAggregate must post nothing
// and return nil. This also confirms the detached-ctx/retry wrapping added by the
// fix did not accidentally start reporting for deploy events.
func TestStatusAggregateDeployEventIsNoOp(t *testing.T) {
	var calls atomic.Int32
	c, ctx, repo, user, pipeline := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	pipeline.Event = model.EventDeploy

	err := c.StatusAggregate(ctx, user, repo, pipeline)
	require.NoError(t, err)
	assert.Zero(t, calls.Load(), "deploy events must not post an aggregate status")
}

// statusMetaFixture wires the meta-aggregate harness, mirroring
// statusAggregateFixture: it sets the meta config (StatusContext "CI",
// StatusMetaContext "{{ .context }} (meta)"), mocks the commit-status POST, and
// returns the client/ctx/repo/user so each test can call StatusMeta directly. It
// does NOT set a pipeline (each meta test builds its own event + workflow set).
// Which workflows are meta gates is now intrinsic (Workflow.OnMetadataEdit), so
// the fixture no longer configures a name list.
func statusMetaFixture(t *testing.T, handler http.HandlerFunc) (*client, context.Context, *model.Repo, *model.User) {
	t.Helper()

	origCtx := server.Config.Server.StatusContext
	origMeta := server.Config.Server.StatusMetaContext
	server.Config.Server.StatusContext = "CI"
	server.Config.Server.StatusMetaContext = "{{ .context }} (meta)"
	t.Cleanup(func() {
		server.Config.Server.StatusContext = origCtx
		server.Config.Server.StatusMetaContext = origMeta
	})

	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatchHandler(
			github_mock.PostReposStatusesByOwnerByRepoBySha,
			handler,
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL}
	repo := &model.Repo{Owner: "o", Name: "r"}
	user := &model.User{AccessToken: "x"}
	return c, ctx, repo, user
}

// TestStatusMetaPostsFilteredRollup verifies the core meta contract: a
// pull_request pipeline carrying a mix of code and meta workflows posts ONE
// status under the event-independent CI (meta) context, whose state rolls up
// ONLY the matching meta gates. Here both meta gates are green while a code
// workflow is (irrelevantly) present, so the meta verdict is success.
func TestStatusMetaPostsFilteredRollup(t *testing.T) {
	var posted github.RepoStatus
	var calls int
	c, ctx, repo, user := statusMetaFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewDecoder(r.Body).Decode(&posted)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}
	workflows := []*model.Workflow{
		{Name: "build", State: model.StatusSuccess},
		{Name: "spec-impact", State: model.StatusSuccess, OnMetadataEdit: true},
		{Name: "pr-title-issue-ref", State: model.StatusSuccess, OnMetadataEdit: true},
	}

	err := c.StatusMeta(ctx, user, repo, pipeline, workflows)
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "exactly one meta status must be posted")
	assert.Equal(t, "CI (meta)", posted.GetContext())
	assert.Equal(t, statusSuccess, posted.GetState())
}

// TestStatusMetaContextIdenticalAcrossEvents is the identity guarantee that lets
// a pull_request_metadata pipeline re-post the SAME required context a
// pull_request pipeline posted: the meta context has no event component, so it is
// byte-identical across the two events. If it were not, the metadata pipeline
// would post to a different context and never mask/refresh the gate.
func TestStatusMetaContextIdenticalAcrossEvents(t *testing.T) {
	capture := func(event model.WebhookEvent) string {
		var posted github.RepoStatus
		c, ctx, repo, user := statusMetaFixture(t, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&posted)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		})
		pipeline := &model.Pipeline{Commit: "abc123", Event: event, Status: model.StatusSuccess}
		workflows := []*model.Workflow{{Name: "spec-impact", State: model.StatusSuccess, OnMetadataEdit: true}}
		require.NoError(t, c.StatusMeta(ctx, user, repo, pipeline, workflows))
		return posted.GetContext()
	}

	pull := capture(model.EventPull)
	metadata := capture(model.EventPullMetadata)
	assert.Equal(t, "CI (meta)", pull)
	assert.Equal(t, pull, metadata, "the meta context must be identical across pull_request and pull_request_metadata")
}

// TestStatusMetaNoMatchingWorkflowPostsNothing pins the no-op: a pipeline whose
// workflows include no configured meta gate must post nothing (the context is
// only ever driven by pipelines that actually carry a gate).
func TestStatusMetaNoMatchingWorkflowPostsNothing(t *testing.T) {
	var calls int
	c, ctx, repo, user := statusMetaFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}
	workflows := []*model.Workflow{
		{Name: "build", State: model.StatusSuccess},
		{Name: "test", State: model.StatusSuccess},
	}

	err := c.StatusMeta(ctx, user, repo, pipeline, workflows)
	require.NoError(t, err)
	assert.Zero(t, calls, "a pipeline carrying no meta gate must post nothing")
}

// TestStatusMetaNoGatePostsNothing pins the intrinsic feature-off corner: a
// pipeline whose workflows are all non-gates (OnMetadataEdit=false) posts
// nothing, even when a workflow's NAME would once have matched a configured
// name list. Gate membership is now the intrinsic column, not a
// name, so a workflow that does not listen on pull_request_metadata never rolls
// up. (The server-level StatusMeta flag that disables the feature outright is
// exercised in server/pipeline/helper_test.go.)
func TestStatusMetaNoGatePostsNothing(t *testing.T) {
	var calls int
	c, ctx, repo, user := statusMetaFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}
	// spec-impact would have matched the old name list, but it is not a gate.
	workflows := []*model.Workflow{{Name: "spec-impact", State: model.StatusSuccess, OnMetadataEdit: false}}

	err := c.StatusMeta(ctx, user, repo, pipeline, workflows)
	require.NoError(t, err)
	assert.Zero(t, calls, "a workflow that is not a meta gate must post nothing regardless of its name")
}

// TestStatusMetaRollsUpOnlyMetaGates is the isolation guarantee: a FAILING meta
// gate with GREEN code workflows must make the meta context red — the meta
// verdict depends ONLY on the gates, never on the code workflows. The pipeline's
// own overall Status is left green to prove StatusMeta rolls up the filtered set,
// not pipeline.Status (which is what StatusAggregate / CI (pr) uses, untouched).
func TestStatusMetaRollsUpOnlyMetaGates(t *testing.T) {
	var posted github.RepoStatus
	c, ctx, repo, user := statusMetaFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&posted)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	// Pipeline overall is success (what CI (pr) would report), but a meta gate failed.
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}
	workflows := []*model.Workflow{
		{Name: "build", State: model.StatusSuccess},
		{Name: "spec-impact", State: model.StatusFailure, OnMetadataEdit: true},
		{Name: "pr-title-issue-ref", State: model.StatusSuccess, OnMetadataEdit: true},
	}

	err := c.StatusMeta(ctx, user, repo, pipeline, workflows)
	require.NoError(t, err)
	assert.Equal(t, "CI (meta)", posted.GetContext())
	assert.Equal(t, statusFailure, posted.GetState(),
		"a failing meta gate must red the meta context even when code workflows and the overall pipeline are green")
}
