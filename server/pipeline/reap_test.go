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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestReapOrphanedWorkflows(t *testing.T) {
	const nowUnix = int64(1_700_000_000)
	const hour = int64(3600)
	now := time.Unix(nowUnix, 0)
	grace := 10 * time.Minute
	reason := "no agent took this workflow within 6h0m0s (pending-timeout); labels: [type=linux]"
	// linuxTask carries a label no agent advertises, plus labels the reason leaves out.
	linuxTask := func(id string, deps ...string) *model.Task {
		return &model.Task{
			ID:           id,
			Labels:       map[string]string{"type": "linux", "repo": "owner/repo", "org-id": "0", "woodpecker-ci.org/repo-id": "200", "empty": ""},
			Dependencies: deps,
			DepStatus:    map[string]model.StatusValue{},
		}
	}
	runsOnFailure := func(task *model.Task) *model.Task {
		task.RunOn = []string{string(model.StatusFailure)}
		return task
	}
	queuedWorkflow := func(id int64) *model.Workflow {
		return &model.Workflow{ID: id, State: model.StatusPending, Children: []*model.Step{{ID: id*10 + 1, State: model.StatusPending}}}
	}
	linuxAgent := func(noSchedule bool, lastContact int64) *model.Agent {
		return &model.Agent{ID: 70, OrgID: model.IDNotSet, NoSchedule: noSchedule, LastContact: lastContact, CustomLabels: map[string]string{"type": "linux"}}
	}

	tests := []struct {
		name              string
		pipelineStatus    model.StatusValue
		pipelineCreated   int64
		pipelineUpdated   int64
		pipelineReviewed  int64
		event             model.WebhookEvent
		workflows         []*model.Workflow
		repoTimeout       int64
		maxTimeout        int64
		queueTasks        []string
		persistedTasks    []string
		queueState        string
		pending           []*model.Task
		waiting           []*model.Task
		running           []*model.Task
		paused            bool
		noPendingTimeout  bool
		taskListErr       error
		missingAgents     map[int64]bool
		agents            map[int64]*model.Agent
		agentList         []*model.Agent
		agentListErr      error
		listAgents        bool
		workflowStates    map[int64]model.StatusValue
		stepStates        map[int64]model.StatusValue
		workflowUpdateErr map[int64]error
		cancels           []string
		cancelErr         error
		done              map[string]model.StatusValue
		errored           []string
		workflowLoads     map[int64]*model.Workflow
		taskDeletes       []string
		timedOut          float64
		aggregate         model.StatusValue
		pipelineFinal     model.StatusValue
		pipelineFinalized bool
		forgeStatus       bool
	}{
		{
			name:            "deleted agent reaps old running workflow and its steps",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 41, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7,
				Children: []*model.Step{
					{ID: 411, State: model.StatusRunning, Started: nowUnix - 3500},
					{ID: 412, State: model.StatusPending},
				},
			}},
			missingAgents:     map[int64]bool{7: true},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusKilled},
			stepStates:        map[int64]model.StatusValue{411: model.StatusKilled, 412: model.StatusCanceled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "pending scheduler task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 42, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7}},
			queueTasks:      []string{fmt.Sprint(42)},
		},
		{
			name:            "persisted task absent from scheduler remains untouched",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 420, State: model.StatusPending}},
			persistedTasks:  []string{fmt.Sprint(420)},
		},
		{
			name:           "task list error fails closed",
			pipelineStatus: model.StatusRunning,
			workflows:      []*model.Workflow{{ID: 421, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 71}},
			taskListErr:    errors.New("task store unavailable"),
			missingAgents:  map[int64]bool{71: true},
		},
		{
			name:            "waiting-on-dependencies queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 49, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7}},
			queueTasks:      []string{fmt.Sprint(49)},
			queueState:      "waiting",
		},
		{
			name:            "running queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 50, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7}},
			queueTasks:      []string{fmt.Sprint(50)},
			queueState:      "running",
		},
		{
			name:            "recent agent within timeout remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 1800,
			workflows:       []*model.Workflow{{ID: 43, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 8}},
			repoTimeout:     60,
			agents:          map[int64]*model.Agent{8: {ID: 8, LastContact: nowUnix - 30}},
		},
		{
			name:              "old agent contact reaps a workflow",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 54, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 15}},
			repoTimeout:       60,
			agents:            map[int64]*model.Agent{15: {ID: 15, LastContact: nowUnix - 900}},
			workflowStates:    map[int64]model.StatusValue{54: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "recently created pending workflow stays within grace",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 300,
			workflows:       []*model.Workflow{{ID: 55, State: model.StatusPending}},
		},
		{
			name:            "recent approval protects pending workflow",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			pipelineUpdated: nowUnix - 300,
			workflows:       []*model.Workflow{{ID: 551, State: model.StatusPending}},
		},
		{
			name:            "pipeline pending age protects a recently started workflow",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 53, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 14}},
			missingAgents:   map[int64]bool{14: true},
		},
		{
			name:            "agent contact exactly at grace cutoff is not old",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 56, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 16}},
			repoTimeout:     60,
			agents:          map[int64]*model.Agent{16: {ID: 16, LastContact: nowUnix - int64(grace.Seconds())}},
		},
		{
			name:            "workflow exactly at age grace cutoff is not old",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 57, State: model.StatusRunning, Started: nowUnix - int64(grace.Seconds()), AgentID: 17}},
			missingAgents:   map[int64]bool{17: true},
		},
		{
			name:            "default timeout remains active until timeout and grace pass",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 52, State: model.StatusRunning, Started: nowUnix - int64((60*time.Minute + 9*time.Minute).Seconds()), AgentID: 13}},
			agents:          map[int64]*model.Agent{13: {ID: 13, LastContact: nowUnix - 30}},
		},
		{
			name:            "max timeout protects live agent workflow",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 1800,
			workflows:       []*model.Workflow{{ID: 443, State: model.StatusRunning, Started: nowUnix - int64((30 * time.Minute).Seconds()), AgentID: 90}},
			repoTimeout:     10,
			maxTimeout:      60,
			agents:          map[int64]*model.Agent{90: {ID: 90, LastContact: nowUnix - 30}},
		},
		{
			name:              "max timeout plus grace reaps live agent workflow",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 4800,
			workflows:         []*model.Workflow{{ID: 444, State: model.StatusRunning, Started: nowUnix - int64((80 * time.Minute).Seconds()), AgentID: 91}},
			repoTimeout:       10,
			maxTimeout:        60,
			agents:            map[int64]*model.Agent{91: {ID: 91, LastContact: nowUnix - 30}},
			workflowStates:    map[int64]model.StatusValue{444: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "orphan younger than grace remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 300,
			workflows:       []*model.Workflow{{ID: 45, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 10}},
			missingAgents:   map[int64]bool{10: true},
		},
		{
			name:            "old pending workflow without agent is canceled",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 46, State: model.StatusPending,
				Children: []*model.Step{{ID: 461, State: model.StatusPending}},
			}},
			workflowStates:    map[int64]model.StatusValue{46: model.StatusCanceled},
			stepStates:        map[int64]model.StatusValue{461: model.StatusCanceled},
			pipelineFinal:     model.StatusCanceled,
			pipelineFinalized: true,
		},
		{
			name:              "reaped started workflow finalizes pipeline as killed",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 461, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 92}},
			missingAgents:     map[int64]bool{92: true},
			workflowStates:    map[int64]model.StatusValue{461: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "two orphan workflows sharing agent use one lookup",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{
				{ID: 462, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 93},
				{ID: 463, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 93},
			},
			missingAgents:     map[int64]bool{93: true},
			workflowStates:    map[int64]model.StatusValue{462: model.StatusKilled, 463: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:              "forge status receives terminal pipeline",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 464, State: model.StatusPending}},
			workflowStates:    map[int64]model.StatusValue{464: model.StatusCanceled},
			pipelineFinal:     model.StatusCanceled,
			pipelineFinalized: true,
			forgeStatus:       true,
		},
		{
			name:            "live workflow keeps pipeline running after another is reaped",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{
				{ID: 47, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 11},
				{ID: 48, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 12},
			},
			agents:         map[int64]*model.Agent{12: {ID: 12, LastContact: nowUnix - 30}},
			missingAgents:  map[int64]bool{11: true},
			workflowStates: map[int64]model.StatusValue{47: model.StatusKilled},
		},
		{
			name:              "active pipeline with only terminal workflows keeps their status",
			pipelineStatus:    model.StatusRunning,
			workflows:         []*model.Workflow{{ID: 51, State: model.StatusFailure}},
			pipelineFinal:     model.StatusFailure,
			pipelineFinalized: true,
		},
		{
			name:              "pending task older than pending-timeout ends error",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			pending:           []*model.Task{linuxTask("41")},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError},
			cancels:           []string{"41"},
			timedOut:          1,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:              "timed out push pipeline reports error aggregate",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			event:             model.EventPush,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			pending:           []*model.Task{linuxTask("41")},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError},
			cancels:           []string{"41"},
			timedOut:          1,
			aggregate:         model.StatusError,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:            "timed out pull workflow beside a leased sibling rolls up error",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 7*hour,
			event:           model.EventPull,
			workflows: []*model.Workflow{
				queuedWorkflow(41),
				{ID: 42, State: model.StatusRunning, Started: nowUnix - hour, AgentID: 7},
			},
			pending:        []*model.Task{linuxTask("41")},
			running:        []*model.Task{{ID: "42"}},
			workflowStates: map[int64]model.StatusValue{41: model.StatusError},
			stepStates:     map[int64]model.StatusValue{411: model.StatusError},
			cancels:        []string{"41"},
			timedOut:       1,
			aggregate:      model.StatusError,
		},
		{
			name:            "timeout skips its default dependent and keeps its failure dependent",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41), queuedWorkflow(43), queuedWorkflow(44)},
			pending:         []*model.Task{linuxTask("41")},
			waiting:         []*model.Task{linuxTask("43", "41"), runsOnFailure(linuxTask("44", "41"))},
			workflowStates:  map[int64]model.StatusValue{41: model.StatusError, 43: model.StatusSkipped},
			stepStates:      map[int64]model.StatusValue{411: model.StatusError, 431: model.StatusCanceled},
			cancels:         []string{"41", "43"},
			timedOut:        1,
		},
		{
			name:            "failure dependent of a timed out and a successful workflow is skipped",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 7*hour,
			workflows: []*model.Workflow{
				queuedWorkflow(41),
				{ID: 45, State: model.StatusSuccess, Started: nowUnix - 2*hour, Finished: nowUnix - hour},
				queuedWorkflow(46),
			},
			pending:           []*model.Task{linuxTask("41")},
			waiting:           []*model.Task{runsOnFailure(linuxTask("46", "41", "45"))},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError, 46: model.StatusSkipped},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError, 461: model.StatusCanceled},
			cancels:           []string{"41", "46"},
			timedOut:          1,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:            "failure dependent waits while its other dependency runs",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 7*hour,
			workflows: []*model.Workflow{
				queuedWorkflow(41),
				{ID: 45, State: model.StatusRunning, Started: nowUnix - hour, AgentID: 7},
				queuedWorkflow(46),
			},
			pending:        []*model.Task{linuxTask("41")},
			waiting:        []*model.Task{runsOnFailure(linuxTask("46", "41", "45"))},
			running:        []*model.Task{{ID: "45"}},
			workflowStates: map[int64]model.StatusValue{41: model.StatusError},
			stepStates:     map[int64]model.StatusValue{411: model.StatusError},
			cancels:        []string{"41"},
			timedOut:       1,
		},
		{
			name:            "dead pending dependent of an old failure is skipped not errored",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 9*hour,
			workflows: []*model.Workflow{
				{ID: 40, State: model.StatusFailure, Started: nowUnix - 9*hour, Finished: nowUnix - 8*hour},
				queuedWorkflow(41),
			},
			pending:           []*model.Task{linuxTask("41", "40")},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusSkipped},
			stepStates:        map[int64]model.StatusValue{411: model.StatusCanceled},
			cancels:           []string{"41"},
			pipelineFinal:     model.StatusFailure,
			pipelineFinalized: true,
		},
		{
			name:              "failed workflow write leaves the task queued",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			pending:           []*model.Task{linuxTask("41")},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError},
			workflowUpdateErr: map[int64]error{41: errors.New("database unavailable")},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError},
			listAgents:        true,
		},
		{
			name:              "failed queue cancel still ends the workflow error",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			pending:           []*model.Task{linuxTask("41")},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError},
			cancels:           []string{"41"},
			cancelErr:         errors.New("failed to delete tasks from persistent store"),
			timedOut:          1,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:              "stale queued row of a successful workflow is marked done",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - hour,
			workflows:         []*model.Workflow{{ID: 41, State: model.StatusSuccess, Started: nowUnix - hour, Finished: nowUnix - 600}},
			persistedTasks:    []string{"41"},
			pending:           []*model.Task{linuxTask("41")},
			done:              map[string]model.StatusValue{"41": model.StatusSuccess},
			pipelineFinal:     model.StatusSuccess,
			pipelineFinalized: true,
		},
		{
			name:              "stale queued row without a workflow is errored and its dependent skipped",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			persistedTasks:    []string{"39"},
			pending:           []*model.Task{linuxTask("39")},
			waiting:           []*model.Task{linuxTask("41", "39")},
			workflowLoads:     map[int64]*model.Workflow{39: nil},
			errored:           []string{"39"},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusSkipped},
			stepStates:        map[int64]model.StatusValue{411: model.StatusCanceled},
			cancels:           []string{"41"},
			pipelineFinal:     model.StatusCanceled,
			pipelineFinalized: true,
		},
		{
			name:            "failure dependent of an unstarted skipped dependency does not time out",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 7*hour,
			workflows: []*model.Workflow{
				{ID: 40, State: model.StatusSkipped},
				queuedWorkflow(41),
			},
			pending: []*model.Task{runsOnFailure(linuxTask("41", "40"))},
		},
		{
			name:            "dependent chain of a timed out workflow is skipped in one pass",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41), queuedWorkflow(42), queuedWorkflow(43)},
			pending:         []*model.Task{linuxTask("41")},
			waiting:         []*model.Task{linuxTask("42", "41"), linuxTask("43", "42")},
			workflowStates: map[int64]model.StatusValue{
				41: model.StatusError, 42: model.StatusSkipped, 43: model.StatusSkipped,
			},
			stepStates: map[int64]model.StatusValue{
				411: model.StatusError, 421: model.StatusCanceled, 431: model.StatusCanceled,
			},
			cancels:           []string{"41", "42", "43"},
			timedOut:          1,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:            "old task waiting on dependencies stays queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			waiting:         []*model.Task{linuxTask("41")},
		},
		{
			name:            "old leased task stays queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			running:         []*model.Task{linuxTask("41")},
		},
		{
			name:            "dependency finished an hour ago keeps the task queued",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 7*hour,
			workflows: []*model.Workflow{
				{ID: 40, State: model.StatusSuccess, Started: nowUnix - 2*hour, Finished: nowUnix - hour},
				queuedWorkflow(41),
			},
			pending: []*model.Task{linuxTask("41", "40")},
		},
		{
			name:             "approval an hour ago keeps the task queued",
			pipelineStatus:   model.StatusPending,
			pipelineCreated:  nowUnix - 7*hour,
			pipelineReviewed: nowUnix - hour,
			workflows:        []*model.Workflow{queuedWorkflow(41)},
			pending:          []*model.Task{linuxTask("41")},
		},
		{
			name:            "five hour old task stays queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 5*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			pending:         []*model.Task{linuxTask("41")},
		},
		{
			name:             "zero pending-timeout keeps the task queued",
			pipelineStatus:   model.StatusPending,
			pipelineCreated:  nowUnix - 7*hour,
			workflows:        []*model.Workflow{queuedWorkflow(41)},
			pending:          []*model.Task{linuxTask("41")},
			noPendingTimeout: true,
		},
		{
			name:            "paused queue keeps the task queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			pending:         []*model.Task{linuxTask("41")},
			paused:          true,
		},
		{
			name:            "busy live agent with matching labels keeps the task queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			pending:         []*model.Task{linuxTask("41")},
			agentList:       []*model.Agent{linuxAgent(false, nowUnix-30)},
			listAgents:      true,
		},
		{
			name:              "matching agent marked no-schedule does not keep the task",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			pending:           []*model.Task{linuxTask("41")},
			agentList:         []*model.Agent{linuxAgent(true, nowUnix-30)},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError},
			cancels:           []string{"41"},
			timedOut:          1,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:              "matching agent last seen beyond the window does not keep the task",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 7*hour,
			workflows:         []*model.Workflow{queuedWorkflow(41)},
			pending:           []*model.Task{linuxTask("41")},
			agentList:         []*model.Agent{linuxAgent(false, nowUnix-7*hour)},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusError},
			stepStates:        map[int64]model.StatusValue{411: model.StatusError},
			cancels:           []string{"41"},
			timedOut:          1,
			pipelineFinal:     model.StatusError,
			pipelineFinalized: true,
		},
		{
			name:            "hostname pinned task stays queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			pending:         []*model.Task{{ID: "41", Labels: map[string]string{"hostname": "builder-1"}}},
		},
		{
			name:            "agent list error keeps the task queued",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 7*hour,
			workflows:       []*model.Workflow{queuedWorkflow(41)},
			pending:         []*model.Task{linuxTask("41")},
			agentListErr:    errors.New("database unavailable"),
			listAgents:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pipelineID = int64(100)
			repo := &model.Repo{ID: 200, UserID: 300, FullName: "owner/repo", Timeout: tt.repoTimeout}
			pipeline := &model.Pipeline{
				ID: pipelineID, RepoID: repo.ID, Number: 13, Event: tt.event,
				Status: tt.pipelineStatus, Created: tt.pipelineCreated, Updated: tt.pipelineUpdated, Reviewed: tt.pipelineReviewed,
			}
			pendingTimeout := 6 * time.Hour
			if tt.noPendingTimeout {
				pendingTimeout = 0
			}

			oldScheduler := server.Config.Services.Scheduler
			oldManager := server.Config.Services.Manager
			oldPerWorkflow := server.Config.Server.StatusPerWorkflow
			oldAggregate := server.Config.Server.StatusAggregate
			oldMeta := server.Config.Server.StatusMeta
			oldMaxTimeout := server.Config.Pipeline.MaxTimeout
			t.Cleanup(func() {
				server.Config.Services.Scheduler = oldScheduler
				server.Config.Services.Manager = oldManager
				server.Config.Server.StatusPerWorkflow = oldPerWorkflow
				server.Config.Server.StatusAggregate = oldAggregate
				server.Config.Server.StatusMeta = oldMeta
				server.Config.Pipeline.MaxTimeout = oldMaxTimeout
			})
			server.Config.Server.StatusPerWorkflow = tt.forgeStatus
			server.Config.Server.StatusAggregate = tt.aggregate != ""
			server.Config.Server.StatusMeta = false
			server.Config.Pipeline.MaxTimeout = tt.maxTimeout

			schedulerMock := scheduler_mocks.NewMockScheduler(t)
			server.Config.Services.Scheduler = schedulerMock
			tasks := make([]*model.Task, 0, len(tt.queueTasks))
			for _, id := range tt.queueTasks {
				tasks = append(tasks, &model.Task{ID: id})
			}
			queueInfo := queue.InfoT{Paused: tt.paused}
			switch tt.queueState {
			case "waiting":
				queueInfo.WaitingOnDeps = tasks
			case "running":
				queueInfo.Running = tasks
			default:
				queueInfo.Pending = tasks
			}
			queueInfo.Pending = append(queueInfo.Pending, tt.pending...)
			queueInfo.WaitingOnDeps = append(queueInfo.WaitingOnDeps, tt.waiting...)
			queueInfo.Running = append(queueInfo.Running, tt.running...)
			queuedIDs := slices.Concat(tt.queueTasks, tt.persistedTasks)
			for _, task := range slices.Concat(tt.pending, tt.waiting, tt.running) {
				queuedIDs = append(queuedIDs, task.ID)
			}

			managerMock := manager_mocks.NewMockManager(t)
			server.Config.Services.Manager = managerMock
			storeMock := store_mocks.NewMockStore(t)
			storeMock.On("GetPipelineQueue").Return([]*model.Feed{{ID: pipeline.ID}}, nil).Once()
			persistedTasks := make([]*model.Task, 0, len(tt.persistedTasks))
			for _, id := range tt.persistedTasks {
				persistedTasks = append(persistedTasks, &model.Task{ID: id})
			}
			taskListCall := storeMock.On("TaskList").Return(persistedTasks, tt.taskListErr).Once()
			if tt.taskListErr == nil {
				schedulerMock.On("Info", mock.Anything).Return(queueInfo).Once().NotBefore(taskListCall)
			}
			if tt.taskListErr == nil {
				storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Once()
				storeMock.On("GetRepo", repo.ID).Return(repo, nil).Once()
				storeMock.On("WorkflowGetTree", pipeline).Return(tt.workflows, nil).Once()
			}

			if tt.taskListErr == nil {
				seenAgents := make(map[int64]struct{})
				for _, workflow := range tt.workflows {
					if slices.Contains(queuedIDs, fmt.Sprint(workflow.ID)) || !workflow.Running() || workflow.AgentID == 0 {
						continue
					}
					if _, seen := seenAgents[workflow.AgentID]; seen {
						continue
					}
					seenAgents[workflow.AgentID] = struct{}{}
					if tt.missingAgents[workflow.AgentID] {
						storeMock.On("AgentFind", workflow.AgentID).Return(nil, store_types.ErrRecordNotExist).Once()
					} else {
						storeMock.On("AgentFind", workflow.AgentID).Return(tt.agents[workflow.AgentID], nil).Once()
					}
				}
			}

			// A timed-out workflow or step gets the reason and a finish time.
			wantFinished := func(started int64, want model.StatusValue) int64 {
				if started != 0 || want == model.StatusError || want == model.StatusSkipped {
					return nowUnix
				}
				return 0
			}
			wantError := func(want model.StatusValue) string {
				if want == model.StatusError {
					return reason
				}
				return ""
			}
			for _, workflow := range tt.workflows {
				if want, reap := tt.workflowStates[workflow.ID]; reap {
					finished := wantFinished(workflow.Started, want)
					storeMock.On("WorkflowUpdate", mock.MatchedBy(func(updated *model.Workflow) bool {
						return updated.ID == workflow.ID && updated.State == want && updated.Finished == finished && updated.Error == wantError(want)
					})).Return(tt.workflowUpdateErr[workflow.ID]).Once()
				}
				for _, step := range workflow.Children {
					want, reap := tt.stepStates[step.ID]
					if !reap {
						continue
					}
					finished := wantFinished(step.Started, want)
					storeMock.On("StepUpdate", mock.MatchedBy(func(updated *model.Step) bool {
						return updated.ID == step.ID && updated.State == want && updated.Finished == finished && updated.Error == wantError(want)
					})).Return(nil).Once()
				}
			}
			for _, id := range tt.cancels {
				schedulerMock.On("CancelWorkflows", mock.Anything, []string{id}).Return(tt.cancelErr).Once()
			}
			for id, state := range tt.done {
				schedulerMock.On("Done", mock.Anything, id, state).Return(nil).Once()
			}
			for _, id := range tt.errored {
				schedulerMock.On("Error", mock.Anything, id, mock.Anything).Return(nil).Once()
			}
			for id, workflow := range tt.workflowLoads {
				if workflow == nil {
					storeMock.On("WorkflowLoad", id).Return(nil, store_types.ErrRecordNotExist).Once()
				} else {
					storeMock.On("WorkflowLoad", id).Return(workflow, nil).Once()
				}
			}
			for _, id := range tt.taskDeletes {
				storeMock.On("TaskDelete", id).Return(nil).Once()
			}
			if tt.timedOut > 0 || tt.listAgents {
				storeMock.On("AgentList", &model.ListOptionsWithAll{All: true}).Return(tt.agentList, tt.agentListErr).Once()
			}

			pipelineUpdated := false
			if tt.pipelineFinalized {
				storeMock.On("UpdatePipeline", mock.MatchedBy(func(updated *model.Pipeline) bool {
					return updated.ID == pipeline.ID && updated.Status == tt.pipelineFinal && updated.Finished == nowUnix
				})).Run(func(mock.Arguments) {
					pipelineUpdated = true
				}).Return(nil).Once()
			}

			changed := tt.pipelineFinalized
			for id := range tt.workflowStates {
				if tt.workflowUpdateErr[id] == nil {
					changed = true
				}
			}
			aggregateForge := &aggregateResilienceForge{statusErrForWorkflowID: -1}
			if changed {
				user := &model.User{ID: repo.UserID}
				storeMock.On("GetUser", repo.UserID).Return(user, nil).Once()
				if !tt.pipelineFinalized {
					// A non-terminal post re-reads the pipeline first (stale-pending guard).
					storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Once()
				}
				switch {
				case tt.forgeStatus:
					forgeMock := forge_mocks.NewMockForge(t)
					forgeMock.On("Status", mock.Anything, user, repo, mock.MatchedBy(func(got *model.Pipeline) bool {
						return got.Status == tt.pipelineFinal && got.Finished == nowUnix
					}), mock.MatchedBy(func(got *model.Workflow) bool {
						return got.ID == 464 && got.State == model.StatusCanceled
					})).Return(nil).Once()
					managerMock.On("ForgeFromRepo", repo).Return(forgeMock, nil).Once()
				case tt.aggregate != "":
					managerMock.On("ForgeFromRepo", repo).Return(aggregateForge, nil).Once()
				default:
					managerMock.On("ForgeFromRepo", repo).Return(nil, nil).Once()
				}
				schedulerMock.On("PublishPipelineEvent", mock.Anything, repo, mock.Anything).Return(nil).Once()
			}

			workflowInitialStates := make(map[int64]model.StatusValue, len(tt.workflows))
			stepInitialStates := make(map[int64]model.StatusValue)
			for _, workflow := range tt.workflows {
				workflowInitialStates[workflow.ID] = workflow.State
				for _, step := range workflow.Children {
					stepInitialStates[step.ID] = step.State
				}
			}
			timedOutBefore := testutil.ToFloat64(pendingWorkflowsTimedOut)
			wantErr := tt.taskListErr
			err := ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout)
			if wantErr != nil {
				require.ErrorIs(t, err, wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.pipelineFinalized, pipelineUpdated)
			assert.InDelta(t, tt.timedOut, testutil.ToFloat64(pendingWorkflowsTimedOut)-timedOutBefore, 0, "timed out counter")
			for _, workflow := range tt.workflows {
				if want, reap := tt.workflowStates[workflow.ID]; reap && tt.workflowUpdateErr[workflow.ID] == nil {
					assert.Equal(t, want, workflow.State, "workflow %d", workflow.ID)
					assert.Equal(t, wantFinished(workflow.Started, want), workflow.Finished, "workflow %d finished", workflow.ID)
					assert.Equal(t, wantError(want), workflow.Error, "workflow %d error", workflow.ID)
				} else {
					assert.Equal(t, workflowInitialStates[workflow.ID], workflow.State, "workflow %d", workflow.ID)
				}
				for _, step := range workflow.Children {
					if want, reap := tt.stepStates[step.ID]; reap {
						assert.Equal(t, want, step.State, "step %d", step.ID)
						assert.Equal(t, wantError(want), step.Error, "step %d error", step.ID)
					} else {
						assert.Equal(t, stepInitialStates[step.ID], step.State, "step %d", step.ID)
					}
				}
			}
			if tt.aggregate != "" {
				require.Equal(t, 1, aggregateForge.aggregateCalls, "aggregate calls")
				if tt.event == model.EventPull {
					assert.Equal(t, tt.aggregate, PipelineStatus(aggregateForge.aggregateWorkflows), "pull aggregate roll-up")
				} else {
					assert.Equal(t, tt.aggregate, aggregateForge.aggregatePipeline.Status, "aggregate pipeline status")
				}
			}
			if changed {
				schedulerMock.AssertNumberOfCalls(t, "PublishPipelineEvent", 1)
			} else {
				schedulerMock.AssertNotCalled(t, "PublishPipelineEvent", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

func TestReapPendingTimeoutNextPass(t *testing.T) {
	const nowUnix = int64(1_700_000_000)
	now := time.Unix(nowUnix, 0)
	grace := 10 * time.Minute
	pendingTimeout := 6 * time.Hour
	reason := "no agent took this workflow within 6h0m0s (pending-timeout); labels: [type=linux]"
	repo := &model.Repo{ID: 200, UserID: 300, FullName: "owner/repo"}
	user := &model.User{ID: repo.UserID}
	linuxTask := func(id string, deps ...string) *model.Task {
		return &model.Task{ID: id, Labels: map[string]string{"type": "linux"}, Dependencies: deps, DepStatus: map[string]model.StatusValue{}}
	}
	isState := func(id int64, state model.StatusValue) any {
		return mock.MatchedBy(func(w *model.Workflow) bool { return w.ID == id && w.State == state })
	}
	isStep := func(id int64, state model.StatusValue) any {
		return mock.MatchedBy(func(s *model.Step) bool { return s.ID == id && s.State == state })
	}
	setup := func(t *testing.T) (*store_mocks.MockStore, *scheduler_mocks.MockScheduler, *manager_mocks.MockManager) {
		oldScheduler := server.Config.Services.Scheduler
		oldManager := server.Config.Services.Manager
		oldPerWorkflow := server.Config.Server.StatusPerWorkflow
		oldAggregate := server.Config.Server.StatusAggregate
		oldMeta := server.Config.Server.StatusMeta
		t.Cleanup(func() {
			server.Config.Services.Scheduler = oldScheduler
			server.Config.Services.Manager = oldManager
			server.Config.Server.StatusPerWorkflow = oldPerWorkflow
			server.Config.Server.StatusAggregate = oldAggregate
			server.Config.Server.StatusMeta = oldMeta
		})
		server.Config.Server.StatusPerWorkflow = false
		server.Config.Server.StatusAggregate = false
		server.Config.Server.StatusMeta = false
		schedulerMock := scheduler_mocks.NewMockScheduler(t)
		managerMock := manager_mocks.NewMockManager(t)
		server.Config.Services.Scheduler = schedulerMock
		server.Config.Services.Manager = managerMock
		return store_mocks.NewMockStore(t), schedulerMock, managerMock
	}

	t.Run("failed dependent write is skipped on the next pass", func(t *testing.T) {
		storeMock, schedulerMock, managerMock := setup(t)
		pipeline := &model.Pipeline{ID: 100, RepoID: repo.ID, Number: 13, Status: model.StatusPending, Created: nowUnix - 7*3600}
		workflows := []*model.Workflow{
			{ID: 41, State: model.StatusPending, Children: []*model.Step{{ID: 411, State: model.StatusPending}}},
			{ID: 42, State: model.StatusPending, Children: []*model.Step{{ID: 421, State: model.StatusPending}}},
		}
		storeMock.On("GetPipelineQueue").Return([]*model.Feed{{ID: pipeline.ID}}, nil).Twice()
		storeMock.On("TaskList").Return([]*model.Task{}, nil).Twice()
		storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil)
		storeMock.On("GetRepo", repo.ID).Return(repo, nil).Twice()
		storeMock.On("WorkflowGetTree", pipeline).Return(workflows, nil).Twice()
		storeMock.On("GetUser", repo.UserID).Return(user, nil).Twice()
		managerMock.On("ForgeFromRepo", repo).Return(nil, nil).Twice()
		schedulerMock.On("PublishPipelineEvent", mock.Anything, repo, mock.Anything).Return(nil).Twice()
		schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{
			Pending: []*model.Task{linuxTask("41")}, WaitingOnDeps: []*model.Task{linuxTask("42", "41")},
		}).Once()
		schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{WaitingOnDeps: []*model.Task{linuxTask("42", "41")}}).Once()

		// Pass 1: workflow 41 times out; its dependent's steps are written, the workflow write fails.
		storeMock.On("AgentList", &model.ListOptionsWithAll{All: true}).Return([]*model.Agent{}, nil).Once()
		storeMock.On("StepUpdate", isStep(411, model.StatusError)).Return(nil).Once()
		storeMock.On("WorkflowUpdate", isState(41, model.StatusError)).Return(nil).Once()
		schedulerMock.On("CancelWorkflows", mock.Anything, []string{"41"}).Return(nil).Once()
		storeMock.On("StepUpdate", isStep(421, model.StatusCanceled)).Return(nil).Once()
		storeMock.On("WorkflowUpdate", isState(42, model.StatusSkipped)).Return(errors.New("database unavailable")).Once()
		require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout))
		assert.Equal(t, model.StatusError, workflows[0].State)
		assert.Equal(t, reason, workflows[0].Error)
		assert.Equal(t, model.StatusPending, workflows[1].State)
		schedulerMock.AssertNotCalled(t, "CancelWorkflows", mock.Anything, []string{"42"})

		// Pass 2: the dependency is already error, so the dependent ends skipped.
		storeMock.On("WorkflowUpdate", mock.MatchedBy(func(w *model.Workflow) bool {
			return w.ID == 42 && w.State == model.StatusSkipped && w.Finished == nowUnix
		})).Return(nil).Once()
		schedulerMock.On("CancelWorkflows", mock.Anything, []string{"42"}).Return(nil).Once()
		storeMock.On("UpdatePipeline", mock.MatchedBy(func(p *model.Pipeline) bool {
			return p.Status == model.StatusError && p.Finished == nowUnix
		})).Return(nil).Once()
		require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout))
		assert.Equal(t, model.StatusSkipped, workflows[1].State)
		assert.Equal(t, model.StatusCanceled, workflows[1].Children[0].State)
	})

	t.Run("failed dependent step write is retried on the next pass", func(t *testing.T) {
		storeMock, schedulerMock, managerMock := setup(t)
		pipeline := &model.Pipeline{ID: 100, RepoID: repo.ID, Number: 13, Status: model.StatusRunning, Created: nowUnix - 7*3600}
		workflows := []*model.Workflow{
			{ID: 40, State: model.StatusFailure, Started: nowUnix - 3*3600, Finished: nowUnix - 2*3600},
			{ID: 42, State: model.StatusPending, Children: []*model.Step{{ID: 421, State: model.StatusPending}}},
		}
		storeMock.On("GetPipelineQueue").Return([]*model.Feed{{ID: pipeline.ID}}, nil).Twice()
		storeMock.On("TaskList").Return([]*model.Task{}, nil).Twice()
		storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Twice()
		storeMock.On("GetRepo", repo.ID).Return(repo, nil).Twice()
		storeMock.On("WorkflowGetTree", pipeline).Return(workflows, nil).Twice()
		schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{WaitingOnDeps: []*model.Task{linuxTask("42", "40")}}).Twice()

		// Pass 1: the step write fails, so the workflow stays pending and queued.
		storeMock.On("StepUpdate", isStep(421, model.StatusCanceled)).Return(errors.New("database unavailable")).Once()
		require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout))
		assert.Equal(t, model.StatusPending, workflows[1].State)
		assert.Equal(t, model.StatusPending, workflows[1].Children[0].State)
		storeMock.AssertNotCalled(t, "WorkflowUpdate", mock.Anything)
		schedulerMock.AssertNotCalled(t, "CancelWorkflows", mock.Anything, mock.Anything)
		schedulerMock.AssertNotCalled(t, "PublishPipelineEvent", mock.Anything, mock.Anything, mock.Anything)

		// Pass 2: the step and workflow are written and the task leaves the queue.
		storeMock.On("StepUpdate", isStep(421, model.StatusCanceled)).Return(nil).Once()
		storeMock.On("WorkflowUpdate", isState(42, model.StatusSkipped)).Return(nil).Once()
		schedulerMock.On("CancelWorkflows", mock.Anything, []string{"42"}).Return(nil).Once()
		storeMock.On("UpdatePipeline", mock.MatchedBy(func(p *model.Pipeline) bool {
			return p.Status == model.StatusFailure && p.Finished == nowUnix
		})).Return(nil).Once()
		storeMock.On("GetUser", repo.UserID).Return(user, nil).Once()
		managerMock.On("ForgeFromRepo", repo).Return(nil, nil).Once()
		schedulerMock.On("PublishPipelineEvent", mock.Anything, repo, mock.Anything).Return(nil).Once()
		require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout))
		assert.Equal(t, model.StatusSkipped, workflows[1].State)
		assert.Equal(t, model.StatusCanceled, workflows[1].Children[0].State)
	})

	t.Run("failed row delete is removed on the next pass", func(t *testing.T) {
		storeMock, schedulerMock, managerMock := setup(t)
		pipeline := &model.Pipeline{ID: 100, RepoID: repo.ID, Number: 13, Status: model.StatusPending, Created: nowUnix - 7*3600}
		workflow := &model.Workflow{ID: 41, State: model.StatusPending, Children: []*model.Step{{ID: 411, State: model.StatusPending}}}
		storeMock.On("GetPipelineQueue").Return([]*model.Feed{{ID: pipeline.ID}}, nil).Once()
		storeMock.On("GetPipelineQueue").Return([]*model.Feed{}, nil).Once()
		storeMock.On("TaskList").Return([]*model.Task{{ID: "41"}}, nil).Twice()
		schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{Pending: []*model.Task{linuxTask("41")}}).Once()
		schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{}).Once()

		// Pass 1: the workflow ends error, the queue fails to delete its row.
		storeMock.On("AgentList", &model.ListOptionsWithAll{All: true}).Return([]*model.Agent{}, nil).Once()
		storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Once()
		storeMock.On("GetRepo", repo.ID).Return(repo, nil).Once()
		storeMock.On("WorkflowGetTree", pipeline).Return([]*model.Workflow{workflow}, nil).Once()
		storeMock.On("StepUpdate", isStep(411, model.StatusError)).Return(nil).Once()
		storeMock.On("WorkflowUpdate", isState(41, model.StatusError)).Return(nil).Once()
		schedulerMock.On("CancelWorkflows", mock.Anything, []string{"41"}).Return(errors.New("failed to delete tasks from persistent store")).Once()
		storeMock.On("UpdatePipeline", mock.Anything).Return(nil).Once()
		storeMock.On("GetUser", repo.UserID).Return(user, nil).Once()
		managerMock.On("ForgeFromRepo", repo).Return(nil, nil).Once()
		schedulerMock.On("PublishPipelineEvent", mock.Anything, repo, mock.Anything).Return(nil).Once()
		require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout))
		assert.Equal(t, model.StatusError, workflow.State)
		storeMock.AssertNotCalled(t, "TaskDelete", mock.Anything)

		// Pass 2: the row outlived the queue, so the reaper deletes it.
		storeMock.On("WorkflowLoad", int64(41)).Return(&model.Workflow{ID: 41, State: model.StatusError}, nil).Once()
		storeMock.On("TaskDelete", "41").Return(nil).Once()
		require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace, pendingTimeout))
		storeMock.AssertNumberOfCalls(t, "TaskDelete", 1)
	})
}
