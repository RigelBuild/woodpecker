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
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// appStatusServer records the Authorization header of every commit-status POST
// and serves the installation lookup and token mint the App path needs.
func appStatusServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var auths []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/o/r/installation", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":99}`))
	})
	mux.HandleFunc("POST /app/installations/99/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"inst-token","expires_at":"2099-01-01T00:00:00Z"}`))
	})
	mux.HandleFunc("POST /repos/o/r/statuses/abc123", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), auths...)
	}
}

// TestRequiredStatusesUseAppToken: with an App configured, CI (pr) and CI
// (meta) post with the installation token, so they never draw on the
// forge user's rate-limit bucket.
func TestRequiredStatusesUseAppToken(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	origCtx := server.Config.Server.StatusContext
	origAgg := server.Config.Server.StatusAggregateFormat
	server.Config.Server.StatusContext = "CI"
	server.Config.Server.StatusAggregateFormat = "{{ .context }} ({{ .event }})"
	t.Cleanup(func() {
		server.Config.Server.StatusContext = origCtx
		server.Config.Server.StatusAggregateFormat = origAgg
	})

	srv, auths := appStatusServer(t)
	c := &client{API: srv.URL + "/", url: srv.URL, appID: 123, appKey: key}
	repo := &model.Repo{Owner: "o", Name: "r"}
	user := &model.User{AccessToken: "user-token"}
	p := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}
	workflows := []*model.Workflow{
		{Name: "build", State: model.StatusSuccess},
		{Name: "title", State: model.StatusSuccess, OnMetadataEdit: true},
	}

	require.NoError(t, c.StatusAggregate(context.Background(), user, repo, p, workflows))
	require.NoError(t, c.StatusMeta(context.Background(), user, repo, p, workflows))

	got := auths()
	require.Len(t, got, 2)
	for _, a := range got {
		assert.Equal(t, "Bearer inst-token", a)
	}
}

// TestRequiredStatusesUseUserTokenWithoutApp keeps the user token when no App
// is configured, the only credential such a server has.
func TestRequiredStatusesUseUserTokenWithoutApp(t *testing.T) {
	origCtx := server.Config.Server.StatusContext
	origAgg := server.Config.Server.StatusAggregateFormat
	server.Config.Server.StatusContext = "CI"
	server.Config.Server.StatusAggregateFormat = "{{ .context }} ({{ .event }})"
	t.Cleanup(func() {
		server.Config.Server.StatusContext = origCtx
		server.Config.Server.StatusAggregateFormat = origAgg
	})

	srv, auths := appStatusServer(t)
	c := &client{API: srv.URL + "/", url: srv.URL}
	repo := &model.Repo{Owner: "o", Name: "r"}
	p := &model.Pipeline{Commit: "abc123", Event: model.EventPull, Status: model.StatusSuccess}

	require.NoError(t, c.StatusAggregate(context.Background(), &model.User{AccessToken: "user-token"}, repo, p,
		[]*model.Workflow{{Name: "build", State: model.StatusSuccess}}))
	assert.Equal(t, []string{"Bearer user-token"}, auths())
}
