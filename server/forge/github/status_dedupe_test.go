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
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-github/v90/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestStatusAggregateSkipsRepeatedState(t *testing.T) {
	var mu sync.Mutex
	var states []string
	c, ctx, repo, user, p := statusAggregateFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var s github.RepoStatus
		_ = json.NewDecoder(r.Body).Decode(&s)
		mu.Lock()
		states = append(states, s.GetState())
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	})
	c.delivered = newStatusDedupe()
	p.Status = model.StatusRunning
	running := []*model.Workflow{{Name: "a", State: model.StatusRunning}, {Name: "b", State: model.StatusPending}}

	// Every workflow transition re-reports; only the rolled-up change is posted.
	for range 5 {
		require.NoError(t, c.StatusAggregate(ctx, user, repo, p, running))
	}
	p.Status = model.StatusSuccess
	done := []*model.Workflow{{Name: "a", State: model.StatusSuccess}, {Name: "b", State: model.StatusSuccess}}
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, done))
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, done))

	assert.Equal(t, []string{statusPending, statusSuccess}, states)
}

func TestStatusAggregateRepostsAfterFailedWrite(t *testing.T) {
	var calls atomic.Int32
	c, ctx, repo, user, p := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		// Every POST of the first report fails; later ones succeed.
		if calls.Add(1) <= forgeWriteMaxAttempts {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})
	c.delivered = newStatusDedupe()
	wf := []*model.Workflow{{Name: "a", State: model.StatusSuccess}}

	require.Error(t, c.StatusAggregate(ctx, user, repo, p, wf))
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, wf))
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, wf))

	assert.Equal(t, int32(forgeWriteMaxAttempts+1), calls.Load(),
		"a failed write must not be remembered; the next report posts once, the one after is skipped")
}

func TestStatusAggregateKeysByPipeline(t *testing.T) {
	var calls atomic.Int32
	c, ctx, repo, user, p := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
	})
	c.delivered = newStatusDedupe()
	wf := []*model.Workflow{{Name: "a", State: model.StatusSuccess}}

	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, wf))
	other := *p
	other.Commit = "def456"
	require.NoError(t, c.StatusAggregate(ctx, user, repo, &other, wf))
	// A restart on the same commit links a new pipeline, so it must post.
	restarted := *p
	restarted.Number++
	require.NoError(t, c.StatusAggregate(ctx, user, repo, &restarted, wf))

	assert.Equal(t, int32(3), calls.Load())
}
