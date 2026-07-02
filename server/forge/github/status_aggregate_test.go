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

	"github.com/google/go-github/v88/github"
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
