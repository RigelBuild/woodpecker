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

//go:build test

package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

// livenessFixture is a real store holding one repo, its owner, and one
// created pipeline. The test clock lies two hours after the pipeline's last
// write, so the pipeline is a stale orphan at that clock.
type livenessFixture struct {
	store    store.Store
	user     *model.User
	repo     *model.Repo
	pipeline *model.Pipeline
	now      time.Time
}

func newLivenessFixture(t *testing.T) *livenessFixture {
	t.Helper()
	s := datastore.NewTestStore(t)

	user := &model.User{Login: "octocat", ForgeRemoteID: "1", Email: "octocat@example.com"}
	require.NoError(t, s.CreateUser(user))
	repo := &model.Repo{
		UserID: user.ID, Owner: "octocat", Name: "hello", FullName: "octocat/hello",
		ForgeRemoteID: "1", ForgeID: 1,
	}
	require.NoError(t, s.CreateRepo(repo))

	p := &model.Pipeline{
		RepoID: repo.ID, Status: model.StatusCreated, Event: model.EventPull,
		Commit: "deadbeef", Branch: "main", Ref: "refs/pull/1/head",
	}
	require.NoError(t, s.CreatePipeline(p))
	p, err := s.GetPipeline(p.ID)
	require.NoError(t, err)

	return &livenessFixture{
		store:    s,
		user:     user,
		repo:     repo,
		pipeline: p,
		now:      time.Unix(p.Updated, 0).Add(2 * time.Hour),
	}
}

func (f *livenessFixture) reload(t *testing.T) *model.Pipeline {
	t.Helper()
	p, err := f.store.GetPipeline(f.pipeline.ID)
	require.NoError(t, err)
	return p
}

// dueReports returns the report rows due at the given time.
func (f *livenessFixture) dueReports(t *testing.T, at time.Time) []*model.PipelineDispatchReport {
	t.Helper()
	reports, err := f.store.DispatchReportsDue(at.Unix(), 100)
	require.NoError(t, err)
	return reports
}

// creatorBetweenClaimAndFlag makes a creator write land after the sweeper
// won the claim and before it flags.
type creatorBetweenClaimAndFlag struct {
	store.Store
	t *testing.T
}

func (s creatorBetweenClaimAndFlag) PipelineDispatchClaim(p *model.Pipeline, now int64) (bool, error) {
	ok, err := s.Store.PipelineDispatchClaim(p, now)
	stored, getErr := s.GetPipeline(p.ID)
	require.NoError(s.t, getErr)
	_, updErr := UpdateToStatusPending(s.Store, *stored, "")
	require.NoError(s.t, updErr)
	return ok, err
}

func TestFlagOrphanedPipeline(t *testing.T) {
	t.Run("stale orphan flags once and gets a report", func(t *testing.T) {
		f := newLivenessFixture(t)
		now := f.now.Unix()

		require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, now))
		stored := f.reload(t)
		assert.Equal(t, model.StatusError, stored.Status)
		assert.EqualValues(t, now, stored.Updated)
		require.Len(t, stored.Errors, 1)
		assert.Equal(t, orphanMessage, stored.Errors[0].Message)

		reports := f.dueReports(t, f.now)
		require.Len(t, reports, 1)
		assert.EqualValues(t, f.pipeline.ID, reports[0].PipelineID)
		assert.EqualValues(t, now, reports[0].FlaggedAt)

		// The same stale view flags nothing more.
		require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, now+1))
		assert.Len(t, f.dueReports(t, f.now.Add(time.Second)), 1)
	})

	t.Run("claim lost to another claimer leaves it created", func(t *testing.T) {
		f := newLivenessFixture(t)
		other := *f.pipeline
		ok, err := f.store.PipelineDispatchClaim(&other, f.now.Unix())
		require.NoError(t, err)
		require.True(t, ok)

		require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, f.now.Unix()+1))
		assert.Equal(t, model.StatusCreated, f.reload(t).Status)
		assert.Empty(t, f.dueReports(t, f.now.Add(time.Hour)))
	})

	t.Run("creator write between claim and flag wins", func(t *testing.T) {
		f := newLivenessFixture(t)
		racing := creatorBetweenClaimAndFlag{Store: f.store, t: t}

		require.NoError(t, flagOrphanedPipeline(racing, f.pipeline, f.now.Unix()))
		assert.Equal(t, model.StatusPending, f.reload(t).Status)
		assert.Empty(t, f.dueReports(t, f.now.Add(time.Hour)))
	})
}
