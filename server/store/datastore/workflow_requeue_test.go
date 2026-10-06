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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func seedRequeueWorkflow(t *testing.T, s *storage, state model.StatusValue, agentID int64) *model.Workflow {
	t.Helper()
	wf := &model.Workflow{
		PipelineID: 1, PID: 1, Name: "build", State: state, AgentID: agentID,
		Started: 100, Finished: 0, Error: "x", Attempts: 1,
		Children: []*model.Step{
			{UUID: "a", PipelineID: 1, PID: 2, PPID: 1, Name: "clone", State: model.StatusSuccess, Started: 100, Finished: 110},
			{UUID: "b", PipelineID: 1, PID: 3, PPID: 1, Name: "test", State: model.StatusRunning, Started: 110, ExitCode: 137, Error: "e"},
		},
	}
	require.NoError(t, s.WorkflowsCreate([]*model.Workflow{wf}))
	loaded, err := s.WorkflowLoad(wf.ID)
	require.NoError(t, err)
	loaded.Children, err = s.StepListFromWorkflowFind(loaded)
	require.NoError(t, err)
	return loaded
}

func TestWorkflowResetForRequeue(t *testing.T) {
	t.Run("running row of the agent resets workflow and steps", func(t *testing.T) {
		s, closer := newTestStore(t, new(model.Step), new(model.Pipeline), new(model.Workflow))
		defer closer()
		wf := seedRequeueWorkflow(t, s, model.StatusRunning, 7)

		ok, err := s.WorkflowResetForRequeue(wf, wf.Children, 7)
		require.NoError(t, err)
		require.True(t, ok)

		row, err := s.WorkflowLoad(wf.ID)
		require.NoError(t, err)
		assert.Equal(t, model.StatusPending, row.State)
		assert.Zero(t, row.AgentID)
		assert.Zero(t, row.Started)
		assert.Empty(t, row.Error)
		assert.Equal(t, 2, row.Attempts)
		steps, err := s.StepListFromWorkflowFind(row)
		require.NoError(t, err)
		for _, step := range steps {
			assert.Equal(t, model.StatusPending, step.State, step.Name)
			assert.Zero(t, step.Started, step.Name)
			assert.Zero(t, step.Finished, step.Name)
			assert.Zero(t, step.ExitCode, step.Name)
			assert.Empty(t, step.Error, step.Name)
		}
		assert.Equal(t, 2, wf.Attempts)
		assert.Equal(t, model.StatusPending, wf.Children[1].State)
	})

	t.Run("pending row only clears the agent", func(t *testing.T) {
		for _, agentID := range []int64{0, 7} {
			t.Run(strconv.FormatInt(agentID, 10), func(t *testing.T) {
				s, closer := newTestStore(t, new(model.Step), new(model.Pipeline), new(model.Workflow))
				defer closer()
				wf := seedRequeueWorkflow(t, s, model.StatusPending, agentID)

				ok, err := s.WorkflowResetForRequeue(wf, wf.Children, 7)
				require.NoError(t, err)
				require.True(t, ok)

				row, err := s.WorkflowLoad(wf.ID)
				require.NoError(t, err)
				assert.Equal(t, model.StatusPending, row.State)
				assert.Zero(t, row.AgentID)
				assert.Equal(t, 1, row.Attempts)
				steps, err := s.StepListFromWorkflowFind(row)
				require.NoError(t, err)
				assert.Equal(t, model.StatusRunning, steps[1].State)
			})
		}
	})

	t.Run("other row is a miss and changes nothing", func(t *testing.T) {
		cases := []struct {
			name    string
			state   model.StatusValue
			agentID int64
		}{
			{"running for another agent", model.StatusRunning, 8},
			{"pending for another agent", model.StatusPending, 8},
			{"finished", model.StatusKilled, 7},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s, closer := newTestStore(t, new(model.Step), new(model.Pipeline), new(model.Workflow))
				defer closer()
				wf := seedRequeueWorkflow(t, s, tc.state, tc.agentID)

				ok, err := s.WorkflowResetForRequeue(wf, wf.Children, 7)
				require.NoError(t, err)
				assert.False(t, ok)

				row, err := s.WorkflowLoad(wf.ID)
				require.NoError(t, err)
				assert.Equal(t, tc.state, row.State)
				assert.Equal(t, tc.agentID, row.AgentID)
				assert.Equal(t, 1, row.Attempts)
				steps, err := s.StepListFromWorkflowFind(row)
				require.NoError(t, err)
				assert.Equal(t, model.StatusRunning, steps[1].State)
			})
		}
	})

	t.Run("stale in-memory row loses the compare-and-set", func(t *testing.T) {
		s, closer := newTestStore(t, new(model.Step), new(model.Pipeline), new(model.Workflow))
		defer closer()
		wf := seedRequeueWorkflow(t, s, model.StatusRunning, 7)
		stale := *wf
		stale.Children = wf.Children

		ok, err := s.WorkflowResetForRequeue(wf, wf.Children, 7)
		require.NoError(t, err)
		require.True(t, ok)
		// A second agent picks the row up again.
		row, err := s.WorkflowLoad(wf.ID)
		require.NoError(t, err)
		row.State, row.AgentID = model.StatusRunning, 9
		require.NoError(t, s.WorkflowUpdate(row))

		ok, err = s.WorkflowResetForRequeue(&stale, stale.Children, 7)
		require.NoError(t, err)
		assert.False(t, ok)
		row, err = s.WorkflowLoad(wf.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(9), row.AgentID)
		assert.Equal(t, 2, row.Attempts)
	})
}
