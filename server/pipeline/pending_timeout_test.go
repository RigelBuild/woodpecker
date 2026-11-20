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
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// A dependency-failed finalize must log the dependency that stopped the workflow.
func TestFinalizeDeadWorkflowsLogsFailedDependency(t *testing.T) {
	tests := []struct {
		name    string
		deps    []int64
		states  map[int64]model.StatusValue
		runOn   []string
		culprit string
	}{
		{
			name:    "single failed dependency",
			deps:    []int64{41},
			states:  map[int64]model.StatusValue{41: model.StatusError},
			culprit: "41",
		},
		{
			name:    "names the failing dependency, not the first",
			deps:    []int64{40, 41},
			states:  map[int64]model.StatusValue{40: model.StatusSuccess, 41: model.StatusFailure},
			culprit: "41",
		},
		{
			name:    "failure-only dependent blocked by a successful dependency",
			deps:    []int64{41},
			states:  map[int64]model.StatusValue{41: model.StatusSuccess},
			runOn:   []string{string(model.StatusFailure)},
			culprit: "41",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := log.Logger
			log.Logger = zerolog.New(&buf).Level(zerolog.InfoLevel)
			t.Cleanup(func() { log.Logger = prev })

			oldScheduler := server.Config.Services.Scheduler
			t.Cleanup(func() { server.Config.Services.Scheduler = oldScheduler })
			schedulerMock := scheduler_mocks.NewMockScheduler(t)
			server.Config.Services.Scheduler = schedulerMock
			storeMock := store_mocks.NewMockStore(t)

			var deps []string
			var workflows []*model.Workflow
			for _, id := range tt.deps {
				deps = append(deps, fmt.Sprint(id))
				workflows = append(workflows, &model.Workflow{ID: id, State: tt.states[id]})
			}
			workflows = append(workflows, &model.Workflow{ID: 46, State: model.StatusPending})
			task := &model.Task{ID: "46", Dependencies: deps, RunOn: tt.runOn}

			storeMock.On("WorkflowUpdateIfState", mock.Anything, model.StatusPending, mock.Anything).Return(nil).Once()
			schedulerMock.On("CancelWorkflows", mock.Anything, []string{"46"}).Return(nil).Once()

			finalized := finalizeDeadWorkflows(t.Context(), storeMock, workflows, map[string]*model.Task{"46": task}, 1_700_000_000, map[int64]*model.Workflow{})
			require.Len(t, finalized, 1)

			var line map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &line), buf.String())
			assert.Equal(t, "finalized queued workflow", line["message"])
			assert.Equal(t, "dependency-failed", line["reason"])
			assert.EqualValues(t, 46, line["workflow_id"])
			assert.Equal(t, tt.culprit, line["dependency_workflow_id"])
			storeMock.AssertExpectations(t)
			schedulerMock.AssertExpectations(t)
		})
	}
}
