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
