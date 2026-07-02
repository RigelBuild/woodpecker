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

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// aggregateResilienceForge is a hand-written fake used to exercise
// updatePipelineStatus. The mockery MockForge cannot serve here: the pipeline
// aggregate is the optional forge.AggregateStatusReporter capability, and
// forge.ReportAggregateStatus type-asserts to it — a type MockForge does not
// satisfy, so the assertion would silently return nil and the aggregate call
// could never be observed. This fake implements both Status and StatusAggregate,
// so ReportAggregateStatus routes through it and we can assert it fired.
//
// It embeds forge.Forge only to satisfy the interface for the fields
// updatePipelineStatus does not touch; any unexpected call on an embedded method
// panics (nil interface), which surfaces accidental new dependencies loudly.
type aggregateResilienceForge struct {
	forge.Forge

	// statusErrForWorkflowID returns an error from Status for exactly one
	// workflow ID, and nil for the rest, so we can simulate a single throttled
	// per-workflow report inside the loop.
	statusErrForWorkflowID int64

	statusCalledFor []int64
	aggregateCalls  int
}

func (f *aggregateResilienceForge) Status(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, workflow *model.Workflow) error {
	f.statusCalledFor = append(f.statusCalledFor, workflow.ID)
	if workflow.ID == f.statusErrForWorkflowID {
		return errors.New("simulated per-workflow status failure (e.g. throttled POST)")
	}
	return nil
}

func (f *aggregateResilienceForge) StatusAggregate(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline) error {
	f.aggregateCalls++
	return nil
}

// setStatusAggregate toggles the process-global StatusAggregate flag for the
// duration of a test, restoring the previous value on cleanup.
func setStatusAggregate(t *testing.T, v bool) {
	t.Helper()
	orig := server.Config.Server.StatusAggregate
	server.Config.Server.StatusAggregate = v
	t.Cleanup(func() { server.Config.Server.StatusAggregate = orig })
}

func threeWorkflowPipeline() (*model.Pipeline, *model.Repo, *model.User) {
	pipeline := &model.Pipeline{
		Number: 1,
		Event:  model.EventPull,
		Status: model.StatusSuccess,
		Workflows: []*model.Workflow{
			{ID: 1, Name: "lint", State: model.StatusSkipped},
			{ID: 2, Name: "test", State: model.StatusSuccess},
			{ID: 3, Name: "build", State: model.StatusSuccess},
		},
	}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}
	return pipeline, repo, user
}

// TestUpdatePipelineStatusAggregateSurvivesPerWorkflowError is the "stuck
// pending" regression. A single per-workflow Status failure (e.g. one throttled
// POST) must NOT abort the loop or skip the pipeline-level aggregate: the
// aggregate is the required branch-protection check, so it has to run even when
// an earlier workflow report failed.
//
// It asserts two things a flipped condition or an early return would break:
//  1. Status was called for EVERY workflow (the loop did not abort on the first
//     error), and
//  2. StatusAggregate was still invoked exactly once despite that error.
//
// Pre-fix relevance: the loop `return`ed on the first Status error, so with the
// first workflow failing, Status ran once (not three times) and the aggregate
// never ran — leaving the required check stuck "pending". Restore that early
// return and both assertions below fail.
func TestUpdatePipelineStatusAggregateSurvivesPerWorkflowError(t *testing.T) {
	setStatusAggregate(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: 1} // first workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"a per-workflow Status error must not abort the loop; every workflow must still be reported")
	assert.Equal(t, 1, f.aggregateCalls,
		"the pipeline-level aggregate must still run exactly once despite a per-workflow error")
}

// TestUpdatePipelineStatusAggregateOnAllSuccess guards the happy path: when every
// per-workflow Status succeeds and StatusAggregate is enabled, the aggregate is
// reported exactly once. This pins that the resilience fix did not accidentally
// double-report or drop the aggregate on the common all-green path.
func TestUpdatePipelineStatusAggregateOnAllSuccess(t *testing.T) {
	setStatusAggregate(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1} // no workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"every workflow must be reported on the happy path")
	assert.Equal(t, 1, f.aggregateCalls,
		"the aggregate must be reported exactly once when StatusAggregate is enabled")
}

// TestUpdatePipelineStatusNoAggregateWhenDisabled pins that the aggregate is
// gated on the StatusAggregate flag: with it off, per-workflow statuses are still
// reported but the aggregate is never called. This keeps the aggregate assertions
// above honest — they prove the flag drives the call, not that it always fires.
func TestUpdatePipelineStatusNoAggregateWhenDisabled(t *testing.T) {
	setStatusAggregate(t, false)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"every workflow must be reported regardless of the aggregate flag")
	require.Zero(t, f.aggregateCalls,
		"the aggregate must not be reported when StatusAggregate is disabled")
}
