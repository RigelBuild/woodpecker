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

package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestDispatchReportsDue(t *testing.T) {
	s, closer := newTestStore(t, new(model.Pipeline), new(model.PipelineDispatchReport))
	defer closer()

	const (
		flagged = int64(1000)
		now     = int64(2000)
	)

	// Each case lives in its own repo so later runs cannot cross over.
	pipelines := []*model.Pipeline{
		// due: fresh held flag, nothing delivered yet
		{ID: 1, RepoID: 1, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
		// due: creator advanced past delivered
		{ID: 2, RepoID: 2, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusPending, Updated: flagged + 5},
		// (3 is deleted)
		// due: creator wrote its own error after the red was delivered
		{ID: 4, RepoID: 4, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged + 5},
		// due: settling
		{ID: 5, RepoID: 5, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusSuccess, Updated: flagged + 5},
		// due: held, later run on same commit and event differs from delivered
		{ID: 6, RepoID: 6, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
		{ID: 60, RepoID: 6, Number: 2, Commit: "c", Event: model.EventPush, Status: model.StatusPending, Updated: flagged},
		// due: held, later run terminal error equals delivered
		{ID: 7, RepoID: 7, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
		{ID: 70, RepoID: 7, Number: 2, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged + 9},
		// not due: held, red delivered, no later run
		{ID: 8, RepoID: 8, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
		// not due: held, nonterminal later run equals delivered
		{ID: 9, RepoID: 9, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
		{ID: 90, RepoID: 9, Number: 2, Commit: "c", Event: model.EventPush, Status: model.StatusRunning, Updated: flagged + 9},
		// not due: held, later runs only on another commit or event
		{ID: 10, RepoID: 10, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
		{ID: 100, RepoID: 10, Number: 2, Commit: "other", Event: model.EventPush, Status: model.StatusPending, Updated: flagged},
		{ID: 101, RepoID: 10, Number: 3, Commit: "c", Event: model.EventPull, Status: model.StatusPending, Updated: flagged},
		// not due: next_attempt in the future
		{ID: 11, RepoID: 11, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusError, Updated: flagged},
	}
	_, err := s.engine.NoAutoTime().Insert(pipelines)
	require.NoError(t, err)

	report := func(id, pipelineID int64, delivered model.StatusValue, next, retire int64) *model.PipelineDispatchReport {
		return &model.PipelineDispatchReport{
			ID: id, PipelineID: pipelineID, RepoID: pipelineID, FlaggedAt: flagged,
			Delivered: delivered, NextAttempt: next, RetireAfter: retire,
		}
	}
	_, err = s.engine.Insert([]*model.PipelineDispatchReport{
		report(1, 1, "", flagged, 0),
		report(2, 2, model.StatusError, flagged+1, 0),
		report(3, 3, model.StatusError, flagged+2, 0),
		report(4, 4, model.StatusError, flagged+3, 0),
		report(5, 5, model.StatusSuccess, flagged+4, flagged+4),
		report(6, 6, model.StatusError, flagged+5, 0),
		report(7, 7, model.StatusError, flagged+6, 0),
		report(8, 8, model.StatusError, flagged, 0),
		report(9, 9, model.StatusRunning, flagged, 0),
		report(10, 10, model.StatusError, flagged, 0),
		report(11, 11, "", now+1, 0),
	})
	require.NoError(t, err)

	ids := func(reports []*model.PipelineDispatchReport) []int64 {
		out := make([]int64, 0, len(reports))
		for _, r := range reports {
			out = append(out, r.ID)
		}
		return out
	}

	due, err := s.DispatchReportsDue(now, 100)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7}, ids(due))

	due, err = s.DispatchReportsDue(now, 2)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, ids(due))
}

func TestPipelineDispatchSuccessor(t *testing.T) {
	s, closer := newTestStore(t, new(model.Pipeline))
	defer closer()

	_, err := s.engine.Insert([]*model.Pipeline{
		{ID: 1, RepoID: 1, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusSuccess},
		{ID: 2, RepoID: 1, Number: 2, Commit: "c", Event: model.EventPush, Status: model.StatusError},
		{ID: 3, RepoID: 1, Number: 3, Commit: "c", Event: model.EventPush, Status: model.StatusFailure},
		{ID: 4, RepoID: 1, Number: 4, Commit: "c", Event: model.EventPush, Status: model.StatusRunning},
		{ID: 5, RepoID: 1, Number: 5, Commit: "d", Event: model.EventPush, Status: model.StatusRunning},
		{ID: 6, RepoID: 1, Number: 6, Commit: "c", Event: model.EventTag, Status: model.StatusRunning},
		{ID: 7, RepoID: 2, Number: 1, Commit: "c", Event: model.EventPush, Status: model.StatusRunning},
	})
	require.NoError(t, err)

	orphan, err := s.GetPipeline(2)
	require.NoError(t, err)
	next, err := s.PipelineDispatchSuccessor(orphan)
	require.NoError(t, err)
	assert.EqualValues(t, 4, next.ID)

	last, err := s.GetPipeline(4)
	require.NoError(t, err)
	_, err = s.PipelineDispatchSuccessor(last)
	assert.ErrorIs(t, err, types.ErrRecordNotExist)
}

func TestDispatchReportLostLease(t *testing.T) {
	s, closer := newTestStore(t, new(model.PipelineDispatchReport))
	defer closer()

	_, err := s.engine.Insert(&model.PipelineDispatchReport{ID: 1, PipelineID: 1, RepoID: 1, NextAttempt: 100})
	require.NoError(t, err)

	load := func() *model.PipelineDispatchReport {
		r := new(model.PipelineDispatchReport)
		ok, err := s.engine.ID(int64(1)).Get(r)
		require.NoError(t, err)
		require.True(t, ok)
		return r
	}

	holderA := load()
	staleA := *holderA
	ok, err := s.DispatchReportClaim(holderA, 400)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, 1, holderA.Claim)
	assert.EqualValues(t, 400, holderA.NextAttempt)

	// A second claim from the stale view loses.
	ok, err = s.DispatchReportClaim(&staleA, 500)
	require.NoError(t, err)
	assert.False(t, ok)

	// After A's lease expires, B claims.
	holderB := load()
	ok, err = s.DispatchReportClaim(holderB, 900)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, 2, holderB.Claim)
	holderB.Delivered = model.StatusPending
	holderB.NextAttempt = 950
	require.NoError(t, s.DispatchReportUpdate(holderB))

	// A's writes are rejected and leave B's row as is.
	holderA.Delivered = model.StatusError
	holderA.NextAttempt = 1
	assert.ErrorIs(t, s.DispatchReportUpdate(holderA), types.ErrRecordNotExist)
	assert.ErrorIs(t, s.DispatchReportDelete(holderA), types.ErrRecordNotExist)

	stored := load()
	assert.Equal(t, model.StatusPending, stored.Delivered)
	assert.EqualValues(t, 950, stored.NextAttempt)
	assert.EqualValues(t, 2, stored.Claim)

	require.NoError(t, s.DispatchReportDelete(holderB))
	n, err := s.engine.Count(new(model.PipelineDispatchReport))
	require.NoError(t, err)
	assert.Zero(t, n)
}
