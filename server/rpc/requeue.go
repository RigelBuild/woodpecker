// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// maxAgentLossRequeues caps how often one workflow is requeued after losing its agent.
const maxAgentLossRequeues = 2

// agentLossRequeueable reports whether a workflow whose agent went away may run again.
func agentLossRequeueable(workflow *model.Workflow, currentPipeline *model.Pipeline) bool {
	if workflow.Attempts >= maxAgentLossRequeues {
		return false
	}
	if currentPipeline.Status == model.StatusKilled || currentPipeline.Status == model.StatusCanceled {
		return false
	}
	for _, step := range workflow.Children {
		if step.State == model.StatusFailure {
			return false
		}
	}
	return hasUnfinishedStep(workflow.Children)
}

// hasUnfinishedStep reports whether a non-detached step has no final result.
func hasUnfinishedStep(steps []*model.Step) bool {
	for _, step := range steps {
		if step.Detached {
			continue
		}
		switch step.State {
		case model.StatusSuccess, model.StatusFailure, model.StatusSkipped:
		default:
			return true
		}
	}
	return false
}

// requeueReserved resets a reserved workflow and puts it back in the queue.
// The handled result is false when the row no longer belongs to agentID; the caller then
// finalizes the workflow, which also releases the reserved entry.
func (s *RPC) requeueReserved(c context.Context, workflow *model.Workflow, currentPipeline *model.Pipeline, repo *model.Repo, agentID int64, state rpc.WorkflowState) (handled bool, err error) {
	strWorkflowID := strconv.FormatInt(workflow.ID, 10)
	firstAttempt := *workflow
	firstSteps := make([]*model.Step, len(workflow.Children))
	for i, step := range workflow.Children {
		stepCopy := *step
		firstSteps[i] = &stepCopy
	}

	ok, err := pipeline.ResetWorkflowForRequeue(s.store, workflow, agentID)
	if err != nil || !ok {
		return false, err
	}

	if err := s.scheduler.Requeue(c, strWorkflowID); err != nil {
		// The entry was released during the reset; keep the first attempt's result.
		log.Warn().Err(err).Str("workflow_id", strWorkflowID).Msg("requeue: entry released during reset, finalizing workflow")
		for _, step := range firstSteps {
			if err := s.store.StepUpdate(step); err != nil {
				log.Error().Err(err).Int64("step_id", step.ID).Msg("requeue: cannot write back step")
			}
		}
		*workflow = firstAttempt
		workflow.Children = firstSteps
		state.Canceled = true
		return true, s.finishWorkflow(c, workflow, currentPipeline, repo, state)
	}

	for _, step := range workflow.Children {
		if err := server.Config.Services.LogStore.LogDelete(step); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, types.ErrRecordNotExist) {
			log.Error().Err(err).Int64("step_id", step.ID).Msg("requeue: cannot delete step logs")
		}
		if err := s.logger.Close(c, step.ID); err != nil && !errors.Is(err, logging.ErrNotFound) {
			log.Error().Err(err).Int64("step_id", step.ID).Msg("requeue: cannot close log stream")
		}
	}

	if currentPipeline.Workflows, err = s.store.WorkflowGetTree(currentPipeline); err != nil {
		log.Error().Err(err).Msg("requeue: cannot build workflow tree")
	} else if err := s.scheduler.PublishPipelineEvent(c, repo, currentPipeline); err != nil {
		log.Error().Err(err).Msg("requeue: cannot publish pipeline event")
	}
	s.reportForgeStatusAsync(c, repo, currentPipeline, workflow)

	log.Info().Str("workflow_id", strWorkflowID).Int("attempts", workflow.Attempts).Msg("requeued workflow after agent loss")
	return true, nil
}

// HandleExpired settles a workflow whose agent stopped extending its lease.
func (s *RPC) HandleExpired(c context.Context, ev queue.ExpiredTask) error {
	workflowID, err := strconv.ParseInt(ev.ID, 10, 64)
	if err != nil {
		return err
	}
	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		return err
	}
	if workflow.Children, err = s.store.StepListFromWorkflowFind(workflow); err != nil {
		return err
	}
	currentPipeline, err := s.store.GetPipeline(workflow.PipelineID)
	if err != nil {
		return err
	}
	repo, err := s.store.GetRepo(currentPipeline.RepoID)
	if err != nil {
		return err
	}

	if err := s.scheduler.Reserve(c, ev.ID, ev.AgentID, true); err != nil {
		if errors.Is(err, queue.ErrNotFound) {
			return nil
		}
		return err
	}

	switch {
	case workflow.State == model.StatusPending && (workflow.AgentID == 0 || workflow.AgentID == ev.AgentID):
		ok, err := pipeline.ResetWorkflowForRequeue(s.store, workflow, ev.AgentID)
		if err != nil {
			// Releasing would orphan the pending row, so dispatch it again.
			return errors.Join(err, s.scheduler.Requeue(c, ev.ID))
		}
		if !ok {
			return s.releaseExpired(c, ev.ID, workflowID)
		}
		if err := s.scheduler.Requeue(c, ev.ID); err != nil && !errors.Is(err, queue.ErrNotFound) {
			return err
		}
		return nil

	case workflow.State == model.StatusRunning && workflow.AgentID == ev.AgentID:
		state := rpc.WorkflowState{Started: workflow.Started, Finished: time.Now().Unix(), Canceled: true}
		if agentLossRequeueable(workflow, currentPipeline) {
			handled, err := s.requeueReserved(c, workflow, currentPipeline, repo, ev.AgentID, state)
			if handled {
				return err
			}
			if err == nil {
				return s.releaseExpired(c, ev.ID, workflowID)
			}
			log.Warn().Err(err).Str("workflow_id", ev.ID).Msg("expired: cannot requeue workflow, finalizing it")
		}
		// A re-run would repeat side effects, so finished steps decide the result.
		if !hasUnfinishedStep(workflow.Children) {
			state.Canceled = false
		}
		return s.finishWorkflow(c, workflow, currentPipeline, repo, state)

	default:
		return s.releaseExpired(c, ev.ID, workflowID)
	}
}

// releaseExpired drops the reserved entry with the state the workflow row ended in.
func (s *RPC) releaseExpired(c context.Context, id string, workflowID int64) error {
	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		return err
	}
	if err := s.scheduler.Done(c, id, workflow.State); err != nil && !errors.Is(err, queue.ErrNotFound) {
		return err
	}
	return nil
}

// consumeExpired hands every lease-expiry event to HandleExpired until c ends.
func (s *RPC) consumeExpired(c context.Context) {
	events := s.scheduler.Expired()
	for {
		select {
		case <-c.Done():
			return
		case ev := <-events:
			if err := s.HandleExpired(c, ev); err != nil {
				log.Error().Err(err).Str("workflow_id", ev.ID).Int64("agent_id", ev.AgentID).Msg("cannot handle expired workflow lease")
			}
		}
	}
}
