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
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// defaultReapTimeout covers a stored timeout of zero, which the agent runs as one hour.
const defaultReapTimeout = 60 * time.Minute

// ReapOrphanedWorkflows finalizes active workflows that cannot finish through an agent.
func ReapOrphanedWorkflows(ctx context.Context, storage store.Store, now time.Time, grace, pendingTimeout time.Duration) error {
	feeds, err := storage.GetPipelineQueue()
	if err != nil {
		return fmt.Errorf("list active pipelines: %w", err)
	}

	persistedTasks, err := storage.TaskList()
	if err != nil {
		log.Error().Err(err).Msg("could not list persisted tasks for orphan reaper")
		return fmt.Errorf("list persisted tasks for orphan reaper: %w", err)
	}

	// Read the store before the scheduler: Poll moves a task to running before
	// deleting its row, so a claimed task is always in one snapshot.
	queueInfo := server.Config.Services.Scheduler.Info(ctx)
	queued := make(map[string]struct{}, len(persistedTasks)+len(queueInfo.Pending)+len(queueInfo.WaitingOnDeps)+len(queueInfo.Running))
	for _, task := range persistedTasks {
		queued[task.ID] = struct{}{}
	}
	for _, tasks := range [][]*model.Task{queueInfo.Pending, queueInfo.WaitingOnDeps, queueInfo.Running} {
		for _, task := range tasks {
			queued[task.ID] = struct{}{}
		}
	}
	pending := make(map[string]*model.Task, len(queueInfo.Pending))
	waiting := make(map[string]*model.Task, len(queueInfo.Pending)+len(queueInfo.WaitingOnDeps))
	for _, task := range queueInfo.Pending {
		pending[task.ID] = task
		waiting[task.ID] = task
	}
	for _, task := range queueInfo.WaitingOnDeps {
		waiting[task.ID] = task
	}
	running := make(map[string]struct{}, len(queueInfo.Running))
	for _, task := range queueInfo.Running {
		running[task.ID] = struct{}{}
	}
	loaded := make(map[string]model.StatusValue)
	// Workflows loaded from outside the walked trees, shared by both dependent checks.
	outsideTree := make(map[int64]*model.Workflow)
	// Live agent filters load once per pass, on the first timeout candidate.
	var liveFilters []scheduler.FilterFn
	var liveErr error
	liveLoaded := false

	type agentResult struct {
		agent *model.Agent
		err   error
	}
	agents := make(map[int64]agentResult)

	for _, feed := range feeds {
		if ctx.Err() != nil {
			return nil
		}
		pipeline, err := storage.GetPipeline(feed.ID)
		if err != nil {
			log.Error().Err(err).Int64("pipeline_id", feed.ID).Msg("could not load active pipeline for orphan reaper")
			continue
		}
		if pipeline.Status != model.StatusRunning && pipeline.Status != model.StatusPending {
			continue
		}
		repo, err := storage.GetRepo(pipeline.RepoID)
		if err != nil {
			log.Error().Err(err).Int64("pipeline_id", pipeline.ID).Msg("could not load repository for orphan reaper")
			continue
		}
		workflows, err := storage.WorkflowGetTree(pipeline)
		if err != nil {
			log.Error().Err(err).Int64("pipeline_id", pipeline.ID).Msg("could not load workflows for orphan reaper")
			continue
		}

		changed := false
		pipelineEverStarted := false
		for _, workflow := range workflows {
			if workflow.Started != 0 {
				pipelineEverStarted = true
			}
			loaded[fmt.Sprint(workflow.ID)] = workflow.State
		}
		for _, workflow := range workflows {
			if !workflow.Running() {
				continue
			}
			if _, exists := queued[fmt.Sprint(workflow.ID)]; exists {
				task, isPending := pending[fmt.Sprint(workflow.ID)]
				if pendingTimeout <= 0 || queueInfo.Paused || !isPending || hostnamePinned(task) {
					continue
				}
				resolved, deleted := withMissingDeps(storage, task, workflows, outsideTree)
				stampUnfinishedDeps(storage, resolved, deleted, task, now.Unix())
				if !pendingTimedOut(pipeline, resolved, deleted, task, now, pendingTimeout) {
					continue
				}
				if !liveLoaded {
					liveFilters, liveErr = loadLiveAgentFilters(storage, now, pendingTimeout)
					liveLoaded = true
					if liveErr != nil {
						log.Error().Err(liveErr).Msg("could not load live agents; no queued workflow times out this pass")
					}
				}
				if liveErr != nil || anyAgentTakes(liveFilters, task) {
					continue
				}
				if !reapWorkflow(storage, workflow, now.Unix(), model.StatusError, pendingTimeoutReason(task, pendingTimeout)) {
					continue
				}
				if err := server.Config.Services.Scheduler.CancelWorkflows(ctx, []string{task.ID}); err != nil {
					log.Error().Err(err).Str("task_id", task.ID).Msg("could not remove timed out workflow from queue")
				}
				changed = true
				pendingWorkflowsTimedOut.Inc()
				log.Info().
					Str("repo", repo.FullName).
					Int64("pipeline", pipeline.Number).
					Int64("workflow_id", workflow.ID).
					Str("reason", "pending-timeout").
					Strs("labels", reasonLabels(task)).
					Msg("finalized queued workflow")
				continue
			}

			// A live agent enforces the real deadline; this is a backstop, so never
			// reap before the longest timeout the workflow could have been given.
			timeout := defaultReapTimeout
			if repo.Timeout != 0 {
				timeout = time.Duration(repo.Timeout) * time.Minute
			}
			if server.Config.Pipeline.MaxTimeout > 0 {
				maxTimeout := time.Duration(server.Config.Pipeline.MaxTimeout) * time.Minute
				if maxTimeout > timeout {
					timeout = maxTimeout
				}
			}
			timedOut := workflow.State == model.StatusRunning && workflow.Started != 0 &&
				now.After(time.Unix(workflow.Started, 0).Add(timeout+grace))

			agentGone := workflow.AgentID == 0
			if workflow.AgentID != 0 {
				result, ok := agents[workflow.AgentID]
				if !ok {
					result.agent, result.err = storage.AgentFind(workflow.AgentID)
					agents[workflow.AgentID] = result
				}
				switch {
				case errors.Is(result.err, store_types.ErrRecordNotExist):
					agentGone = true
				case result.err != nil:
					log.Error().Err(result.err).Int64("agent_id", workflow.AgentID).Msg("could not load agent for orphan reaper")
				case result.agent != nil && time.Unix(result.agent.LastContact, 0).Before(now.Add(-grace)):
					agentGone = true
				}
			}

			anchor := workflow.Started
			if workflow.State == model.StatusPending && anchor == 0 {
				anchor = pipeline.Updated
				if anchor == 0 {
					anchor = pipeline.Created
				}
			}
			oldEnough := anchor != 0 && time.Unix(anchor, 0).Before(now.Add(-grace))
			if !timedOut && (!agentGone || !oldEnough) {
				continue
			}

			reason := "agent-gone"
			if timedOut {
				reason = "timeout"
			}
			if !reapWorkflow(storage, workflow, now.Unix(), model.StatusCanceled, "") {
				continue
			}
			changed = true
			log.Info().
				Str("repo", repo.FullName).
				Int64("pipeline", pipeline.Number).
				Int64("workflow_id", workflow.ID).
				Str("reason", reason).
				Msg("reaped orphaned workflow")
		}
		if pendingTimeout > 0 && len(finalizeDeadWorkflows(ctx, storage, workflows, waiting, now.Unix(), outsideTree)) > 0 {
			changed = true
		}

		if !model.IsThereRunningStage(workflows) {
			status := PipelineStatus(workflows)
			// A never-started canceled pipeline posts success, so an error keeps its status.
			hasError := slices.ContainsFunc(workflows, func(workflow *model.Workflow) bool { return workflow.State == model.StatusError })
			if changed && !pipelineEverStarted && !hasError {
				status = model.StatusCanceled
			}
			updated, err := UpdateStatusToDone(storage, *pipeline, status, now.Unix())
			if err != nil {
				log.Error().Err(err).Int64("pipeline_id", pipeline.ID).Msg("could not finalize pipeline in orphan reaper")
			} else {
				pipeline = updated
				changed = true
			}
		}
		if !changed {
			continue
		}

		pipeline.Workflows = workflows
		user, err := storage.GetUser(repo.UserID)
		if err != nil {
			log.Error().Err(err).Str("repo", repo.FullName).Msg("could not load repository owner for orphan status report")
		} else if forge, forgeErr := server.Config.Services.Manager.ForgeFromRepo(repo); forgeErr != nil {
			log.Error().Err(forgeErr).Str("repo", repo.FullName).Msg("could not load forge for orphan status report")
		} else {
			updatePipelineStatus(ctx, forge, storage, pipeline, repo, user)
		}
		if err := server.Config.Services.Scheduler.PublishPipelineEvent(ctx, repo, pipeline); err != nil {
			log.Error().Err(err).Str("repo", repo.FullName).Int64("pipeline", pipeline.Number).Msg("could not publish reaped pipeline status change")
		}
	}

	if pendingTimeout > 0 {
		removeStaleTaskRows(ctx, storage, persistedTasks, loaded, waiting, running, outsideTree)
	}
	return nil
}

func reapWorkflow(storage store.Store, workflow *model.Workflow, finished int64, pendingState model.StatusValue, errMsg string) bool {
	for _, step := range workflow.Children {
		if !step.Running() {
			continue
		}
		updated := *step
		if updated.Started != 0 {
			updated.State = model.StatusKilled
			updated.Finished = finished
		} else {
			updated.State = pendingState
		}
		if errMsg != "" {
			updated.Error = errMsg
			updated.Finished = finished
		}
		if err := storage.StepUpdate(&updated); err != nil {
			log.Error().Err(err).Int64("step_id", step.ID).Msg("could not finalize orphaned workflow step")
			return false
		}
		*step = updated
	}

	updated := *workflow
	if updated.Started != 0 && errMsg == "" {
		updated.State = model.StatusKilled
		updated.Finished = finished
	} else {
		// A pending timeout ends the workflow in pendingState even when an expired lease once started it.
		updated.State = pendingState
	}
	if errMsg != "" {
		updated.Error = errMsg
		updated.Finished = finished
	}
	if err := storage.WorkflowUpdate(&updated); err != nil {
		log.Error().Err(err).Int64("workflow_id", workflow.ID).Msg("could not finalize orphaned workflow")
		return false
	}
	*workflow = updated
	return true
}

// pendingTimedOut reports whether a runnable task has waited longer than the timeout.
func pendingTimedOut(p *model.Pipeline, workflows []*model.Workflow, deleted map[int64]bool, task *model.Task, now time.Time, timeout time.Duration) bool {
	deps, ok := treeDepStatus(task, workflows)
	if !ok || !(&model.Task{RunOn: task.RunOn, DepStatus: deps}).ShouldRun() {
		return false
	}
	since := runnableSince(p, workflows, deleted, task, now.Unix())
	return since > 0 && time.Unix(since, 0).Before(now.Add(-timeout))
}
