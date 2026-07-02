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

// setReportSkippedWorkflows sets the ReportSkippedWorkflows flag for the duration
// of a test, restoring the previous value on cleanup. The flag is process-global
// (server.Config), so every test that depends on it must save/restore.
func setReportSkippedWorkflows(t *testing.T, v bool) {
	t.Helper()
	orig := server.Config.Server.ReportSkippedWorkflows
	server.Config.Server.ReportSkippedWorkflows = v
	t.Cleanup(func() { server.Config.Server.ReportSkippedWorkflows = orig })
}

// skippedGateAppClient wires an App-configured client (appConfigured() == true)
// whose Checks-API endpoints are all mocked, and whose check-run POST increments
// posts. It mirrors the harness in checks_test.go's TestStatusChecksAPISkipped.
//
// The skipped-gate under test sits BEFORE appConfigured(), so when the gate fires
// none of these endpoints are reached and posts stays 0; when it does not fire the
// report walks the Checks API and CreateCheckRun bumps posts. That lets a single
// counter distinguish "dropped by the gate" from "reported".
func skippedGateAppClient(t *testing.T, posts *atomic.Int32) (*client, context.Context) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

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
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				posts.Add(1)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}),
		),
	)
	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), githubClientKey, gh)

	c := &client{API: defaultAPI, url: defaultURL, appID: 123, appKey: key}
	return c, ctx
}

// TestStatusSkippedGateDropsSkippedWhenReportingDisabled is the core rate-limit
// regression: with ReportSkippedWorkflows off (the default), a skipped workflow
// must be dropped BEFORE any forge write, so it produces zero check-run POSTs.
// The production bug was that every one of the dozens of skipped workflows an
// affected-aware fan-out carries became a check-run POST, tripping GitHub's
// secondary rate limit and leaving the required check stuck "pending".
//
// Pre-fix relevance: remove the skipped-gate in github.go and this skipped
// workflow walks straight into createOrUpdateCheckRun, so posts becomes 1 and
// this assertion (posts == 0) fails. That is the RED that proves the gate has
// teeth.
func TestStatusSkippedGateDropsSkippedWhenReportingDisabled(t *testing.T) {
	setReportSkippedWorkflows(t, false)

	var posts atomic.Int32
	c, ctx := skippedGateAppClient(t, &posts)
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	workflow := &model.Workflow{ID: 7, Name: "lint", State: model.StatusSkipped}

	err := c.Status(ctx, &model.User{AccessToken: "x"}, repo, pipeline, workflow)
	require.NoError(t, err)
	assert.Equal(t, int32(0), posts.Load(),
		"a skipped workflow must make no forge check-run POST when ReportSkippedWorkflows is off")
}

// TestStatusSkippedGateReportsNonSkippedWhenReportingDisabled pins the other side
// of the gate's specificity: the drop is skipped-ONLY. With reporting off, a
// non-skipped workflow (success) must still be reported — the gate must not
// suppress real statuses. Same App/check-run path as the case above, so together
// they isolate workflow.State as the only thing that changes the outcome.
//
// Pre-fix relevance: this passes with and without the gate (the gate never fires
// for a success workflow); it exists to guard against a future "drop everything"
// over-broadening of the gate, which would drop this POST and redden the test.
func TestStatusSkippedGateReportsNonSkippedWhenReportingDisabled(t *testing.T) {
	setReportSkippedWorkflows(t, false)

	var posts atomic.Int32
	c, ctx := skippedGateAppClient(t, &posts)
	repo := &model.Repo{Owner: "o", Name: "r"}
	pipeline := &model.Pipeline{Commit: "abc123", Event: model.EventPush}
	workflow := &model.Workflow{ID: 7, Name: "lint", State: model.StatusSuccess}

	err := c.Status(ctx, &model.User{AccessToken: "x"}, repo, pipeline, workflow)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, posts.Load(), int32(1),
		"a non-skipped workflow must still be reported even when ReportSkippedWorkflows is off")
}
