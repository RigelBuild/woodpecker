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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-github/v92/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// recordStates returns a handler that records each posted state and the
// number of POSTs to fail with 502 before succeeding.
func recordStates(states *[]string, failFirst int32) http.HandlerFunc {
	var mu sync.Mutex
	var calls atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		var s github.RepoStatus
		_ = json.NewDecoder(r.Body).Decode(&s)
		mu.Lock()
		*states = append(*states, s.GetState())
		mu.Unlock()
		if calls.Add(1) <= failFirst {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
}

var (
	runningTree = []*model.Workflow{{Name: "a", State: model.StatusRunning}, {Name: "b", State: model.StatusPending}}
	successTree = []*model.Workflow{{Name: "a", State: model.StatusSuccess}, {Name: "b", State: model.StatusSuccess}}
)

func TestStatusAggregateSkipsRepeatedPending(t *testing.T) {
	var states []string
	c, ctx, repo, user, p := statusAggregateFixture(t, recordStates(&states, 0))
	c.delivered = newStatusDedupe()
	p.Status = model.StatusRunning

	// Every workflow transition re-reports; repeated pending is posted once.
	for range 5 {
		require.NoError(t, c.StatusAggregate(ctx, user, repo, p, runningTree))
	}
	// Terminal states always post, so a check another writer changed heals.
	p.Status = model.StatusSuccess
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, successTree))
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, successTree))

	assert.Equal(t, []string{statusPending, statusSuccess, statusSuccess}, states)
}

func TestStatusAggregateRepostsAfterFailedWrite(t *testing.T) {
	// First report succeeds; every attempt of the second fails (GitHub may
	// still have applied it); the third repeats the first and must post.
	var calls atomic.Int32
	c, ctx, repo, user, p := statusAggregateFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		if n := calls.Add(1); n >= 2 && n <= 1+forgeWriteMaxAttempts {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})
	c.delivered = newStatusDedupe()
	p.Status = model.StatusRunning
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, runningTree))
	failing := *p
	failing.Number++
	require.Error(t, c.StatusAggregate(ctx, user, repo, &failing, runningTree))
	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, runningTree))

	assert.Equal(t, int32(2+forgeWriteMaxAttempts), calls.Load(),
		"a failed write must clear the remembered state so the repeat posts")
}

func TestStatusAggregateKeysByCommitAndPipeline(t *testing.T) {
	var states []string
	c, ctx, repo, user, p := statusAggregateFixture(t, recordStates(&states, 0))
	c.delivered = newStatusDedupe()
	p.Status = model.StatusRunning

	require.NoError(t, c.StatusAggregate(ctx, user, repo, p, runningTree))
	other := *p
	other.Commit = "def456"
	require.NoError(t, c.StatusAggregate(ctx, user, repo, &other, runningTree))
	// A restart on the same commit links a new pipeline, so it must post.
	restarted := *p
	restarted.Number++
	require.NoError(t, c.StatusAggregate(ctx, user, repo, &restarted, runningTree))

	assert.Len(t, states, 3)
}

func TestStatusMetaSkipsRepeatedPending(t *testing.T) {
	var states []string
	c, ctx, repo, user, p := statusAggregateFixture(t, recordStates(&states, 0))
	orig := server.Config.Server.StatusMetaContext
	server.Config.Server.StatusMetaContext = "{{ .context }} (meta)"
	t.Cleanup(func() { server.Config.Server.StatusMetaContext = orig })
	c.delivered = newStatusDedupe()
	p.Status = model.StatusRunning
	meta := []*model.Workflow{{Name: "m", State: model.StatusRunning, OnMetadataEdit: true}}

	require.NoError(t, c.StatusMeta(ctx, user, repo, p, meta))
	require.NoError(t, c.StatusMeta(ctx, user, repo, p, meta))
	// A metadata pipeline on the same commit has its own URL, so it posts.
	edit := *p
	edit.Number++
	require.NoError(t, c.StatusMeta(ctx, user, repo, &edit, meta))

	assert.Equal(t, []string{statusPending, statusPending}, states)
}

func TestStatusDedupeSharedAcrossForgeRebuilds(t *testing.T) {
	a, err := New(1, Opts{URL: "https://ghe.dedupe.test"})
	require.NoError(t, err)
	b, err := New(1, Opts{URL: "https://ghe.dedupe.test"})
	require.NoError(t, err)
	assert.Same(t, a.(*client).delivered, b.(*client).delivered) //nolint:forcetypeassert
}

func TestStatusDedupeSerializesOneKey(t *testing.T) {
	d := newStatusDedupe()
	ctx := context.Background()
	inSend := make(chan struct{})
	release := make(chan struct{})
	var secondBuilt atomic.Bool

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = d.post(ctx, "k", func() (github.RepoStatus, error) {
			return github.RepoStatus{State: new(statusPending)}, nil
		}, func(github.RepoStatus) (*github.Response, error) {
			close(inSend)
			<-release
			return nil, nil
		})
	}()
	<-inSend

	second := make(chan struct{})
	go func() {
		defer close(second)
		_, _ = d.post(ctx, "k", func() (github.RepoStatus, error) {
			secondBuilt.Store(true)
			return github.RepoStatus{State: new(statusSuccess)}, nil
		}, func(github.RepoStatus) (*github.Response, error) { return nil, nil })
	}()
	// Wait until the second report is queued on the key, so a broken lock
	// would let its build run before the assertion below.
	require.Eventually(t, func() bool { return lockRefs(d, "k") == 2 }, time.Second, time.Millisecond)
	// Another key is not blocked by the held one.
	_, err := d.post(ctx, "other", func() (github.RepoStatus, error) {
		return github.RepoStatus{State: new(statusPending)}, nil
	}, func(github.RepoStatus) (*github.Response, error) { return nil, nil })
	require.NoError(t, err)
	assert.False(t, secondBuilt.Load(), "a second report for the key must wait for the first send")

	close(release)
	<-done
	<-second
	assert.True(t, secondBuilt.Load())
	assert.Empty(t, d.locks, "released key locks must be dropped")
}

func TestStatusDedupeLockHonorsContext(t *testing.T) {
	d := newStatusDedupe()
	unlock, err := d.lock(context.Background(), "k")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = d.post(ctx, "k", func() (github.RepoStatus, error) {
		t.Fatal("build must not run without the lock")
		return github.RepoStatus{}, nil
	}, nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, lockRefs(d, "k"), "a timed-out waiter must drop its ref")
	unlock()
	assert.Equal(t, 0, lockRefs(d, "k"))
}

func lockRefs(d *statusDedupe, key string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if l := d.locks[key]; l != nil {
		return l.refs
	}
	return 0
}
