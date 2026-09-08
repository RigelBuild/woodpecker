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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-github/v90/github"
	github_mock "github.com/migueleliasweb/go-github-mock/src/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/github/fixtures"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestUsablePushBase(t *testing.T) {
	const curr = "366701fde727cb7a9e7f21eb88264f59f6f9b89c"

	const prev = "2f780193b136b72bfea4eeb640786a8c4450c7a2"

	tests := []struct {
		name string
		curr string
		prev string
		want bool
	}{
		{name: "distinct previous head is a usable base", curr: curr, prev: prev, want: true},
		{name: "all-zero SHA is not a usable base", curr: curr, prev: zeroSHA},
		{name: "previous head equal to current is not a usable base", curr: curr, prev: curr},
		{name: "empty previous head is not a usable base", curr: curr, prev: ""},
		{name: "empty current head is not a usable base", curr: "", prev: prev},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, usablePushBase(tc.curr, tc.prev))
		})
	}
}

// jsonHandler serves body as JSON on every request, unlike the single-shot FIFO
// entry WithRequestMatch installs.
func jsonHandler(t *testing.T, body any) http.Handler {
	t.Helper()

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("encoding mocked GitHub response: %v", err)
		}
	})
}

// pushHookWithBefore rewrites the "before" commit of the sample push hook,
// leaving the rest of the payload untouched. GitHub varies only that field
// between an ordinary push, a ref creation and a no-op push.
func pushHookWithBefore(t *testing.T, before string) string {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(fixtures.HookPush), &payload))
	payload["before"] = before

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return string(raw)
}

// pushBaseTestClient wires a client whose changed-file lookups are mocked and
// whose store resolves to a fake repo and user, mirroring the harness in
// TestHook. Only pipeline.Before is under test here; the changed-file payloads
// exist so Hook can run to completion, so the endpoints are served by handlers
// that answer any number of times rather than the FIFO queue WithRequestMatch
// installs, which a table of subtests would exhaust.
func pushBaseTestClient(t *testing.T) (*client, context.Context) {
	t.Helper()

	changedFile := []*github.CommitFile{{Filename: github.Ptr("main.go")}}
	mockedHTTPClient := github_mock.NewMockedHTTPClient(
		github_mock.WithRequestMatchHandler(
			github_mock.GetReposCommitsByOwnerByRepoByRef,
			jsonHandler(t, github.RepositoryCommit{Files: changedFile}),
		),
		github_mock.WithRequestMatchHandler(
			github_mock.GetReposCompareByOwnerByRepoByBasehead,
			jsonHandler(t, github.CommitsComparison{Files: changedFile}),
		),
	)

	gh, err := github.NewClient(github.WithHTTPClient(mockedHTTPClient))
	require.NoError(t, err)

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("GetUser", mock.Anything).Return(&model.User{ID: 1, Login: "6543", AccessToken: "token"}, nil).Maybe()
	mockStore.On("GetRepoNameFallback", mock.Anything, mock.Anything, mock.Anything).Return(&model.Repo{
		ID:            1,
		ForgeRemoteID: "1",
		Owner:         "6543",
		Name:          "hello-world",
		UserID:        1,
	}, nil).Maybe()

	ctx := context.WithValue(t.Context(), githubClientKey, gh)
	ctx = store.InjectToContext(ctx, mockStore)

	return &client{API: defaultAPI, url: defaultURL}, ctx
}

func TestHookPushBefore(t *testing.T) {
	const head = "366701fde727cb7a9e7f21eb88264f59f6f9b89c"

	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "ordinary push carries the previous head",
			payload: fixtures.HookPush,
			want:    "2f780193b136b72bfea4eeb640786a8c4450c7a2",
		},
		{
			name:    "branch creation drops the all-zero base",
			payload: pushHookWithBefore(t, zeroSHA),
		},
		{
			name:    "push that does not move the ref drops the base",
			payload: pushHookWithBefore(t, head),
		},
		{
			name:    "force push drops the rewritten base",
			payload: fixtures.HookPushForced,
		},
	}

	c, ctx := pushBaseTestClient(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/hook", strings.NewReader(tc.payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-GitHub-Event", "push")

			_, pipeline, err := c.Hook(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, pipeline)
			require.Equal(t, model.EventPush, pipeline.Event)
			assert.Equal(t, tc.want, pipeline.Before)
		})
	}
}
