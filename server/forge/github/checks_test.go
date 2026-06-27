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

	"github.com/google/go-github/v88/github"
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
