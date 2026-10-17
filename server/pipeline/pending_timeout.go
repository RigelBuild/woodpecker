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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

var pendingWorkflowsTimedOut = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "woodpecker",
	Name:      "pending_workflows_timed_out_total",
	Help:      "Queued workflows ended error because no agent took them within pending-timeout.",
})

// loadLiveAgentFilters returns a label filter for each agent that can take work now.
func loadLiveAgentFilters(storage store.Store, now time.Time, window time.Duration) ([]scheduler.FilterFn, error) {
	agents, err := storage.AgentList(&model.ListOptionsWithAll{All: true})
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	cutoff := now.Add(-window)
	filters := make([]scheduler.FilterFn, 0, len(agents))
	for _, agent := range agents {
		if agent.NoSchedule || time.Unix(agent.LastContact, 0).Before(cutoff) {
			continue
		}
		labels, err := agent.PollLabels()
		if err != nil {
			return nil, fmt.Errorf("labels of agent %d: %w", agent.ID, err)
		}
		wildcard, err := explicitHostnameWildcard(agent)
		if err != nil {
			return nil, fmt.Errorf("server labels of agent %d: %w", agent.ID, err)
		}
		filters = append(filters, hostnameAwareFilter(scheduler.NewLabelFilter(labels), wildcard))
	}
	return filters, nil
}

// explicitHostnameWildcard reports whether a server filter, or else a custom label, sets hostname "*".
// The "*" PollLabels uses as a default stands in for the concrete hostname a real agent polls with.
func explicitHostnameWildcard(agent *model.Agent) (bool, error) {
	serverLabels, err := agent.GetServerLabels()
	if err != nil {
		return false, err
	}
	if hostname, ok := serverLabels[pipeline.LabelFilterHostname]; ok {
		return hostname == "*", nil
	}
	return agent.CustomLabels[pipeline.LabelFilterHostname] == "*", nil
}

// hostnameAwareFilter rejects a task with hostname "*" unless the agent explicitly sets hostname "*".
// A real agent polls with its concrete hostname, which the scheduler never matches against a task's "*".
func hostnameAwareFilter(filter scheduler.FilterFn, agentHostnameWildcard bool) scheduler.FilterFn {
	return func(task *model.Task) (bool, int) {
		if task.Labels[pipeline.LabelFilterHostname] == "*" && !agentHostnameWildcard {
			return false, 0
		}
		return filter(task)
	}
}

// anyAgentTakes reports whether one of the filters matches the task.
func anyAgentTakes(filters []scheduler.FilterFn, task *model.Task) bool {
	return slices.ContainsFunc(filters, func(filter scheduler.FilterFn) bool {
		matched, _ := filter(task)
		return matched
	})
}

// hostnamePinned reports whether the task names one agent host; such a task never times out.
func hostnamePinned(task *model.Task) bool {
	hostname := task.Labels[pipeline.LabelFilterHostname]
	return hostname != "" && hostname != "*"
}

// stampUnfinishedDeps persists now as the finish time of each terminal dependency that has none.
// The stored value anchors the pending clock; a failed write is retried next pass.
func stampUnfinishedDeps(storage store.Store, workflows []*model.Workflow, deleted map[int64]bool, task *model.Task, now int64) {
	for _, workflow := range workflows {
		if workflow.Finished != 0 || workflow.Running() || deleted[workflow.ID] || !slices.Contains(task.Dependencies, fmt.Sprint(workflow.ID)) {
			continue
		}
		updated := *workflow
		updated.Finished = now
		if err := storage.WorkflowUpdate(&updated); err != nil {
			log.Error().Err(err).Int64("workflow_id", workflow.ID).Msg("could not stamp finish time of dependency")
			continue
		}
		*workflow = updated
	}
}

// runnableSince returns when the task became runnable, or 0 while a dependency is missing or active.
// A dependency without a finish time gives now; a deleted one adds no time.
func runnableSince(p *model.Pipeline, workflows []*model.Workflow, deleted map[int64]bool, task *model.Task, now int64) int64 {
	if _, ok := treeDepStatus(task, workflows); !ok {
		return 0
	}
	since := max(p.Created, p.Reviewed)
	for _, workflow := range workflows {
		if deleted[workflow.ID] || !slices.Contains(task.Dependencies, fmt.Sprint(workflow.ID)) {
			continue
		}
		if workflow.Finished == 0 {
			return now
		}
		since = max(since, workflow.Finished)
	}
	return since
}

// pendingTimeoutReason names the timeout and the task labels an agent must match.
func pendingTimeoutReason(task *model.Task, timeout time.Duration) string {
	return fmt.Sprintf("no agent took this workflow within %s (pending-timeout); labels: [%s]", timeout, strings.Join(reasonLabels(task), ", "))
}

func reasonLabels(task *model.Task) []string {
	labels := make([]string, 0, len(task.Labels))
	for key, value := range task.Labels {
		if value == "" || key == pipeline.LabelFilterRepo || key == pipeline.LabelFilterOrg ||
			strings.HasPrefix(key, pipeline.InternalLabelPrefix+"/") {
			continue
		}
		labels = append(labels, key+"="+value)
	}
	slices.Sort(labels)
	return labels
}

// treeDepStatus maps each dependency to its workflow state; false while one is missing or active.
func treeDepStatus(task *model.Task, workflows []*model.Workflow) (map[string]model.StatusValue, bool) {
	deps := make(map[string]model.StatusValue, len(task.Dependencies))
	for _, dep := range task.Dependencies {
		index := slices.IndexFunc(workflows, func(workflow *model.Workflow) bool { return fmt.Sprint(workflow.ID) == dep })
		if index < 0 || workflows[index].Running() {
			return nil, false
		}
		deps[dep] = workflows[index].State
	}
	return deps, true
}

// loadOutsideTree loads a workflow absent from the walked tree once per pass; nil means it does not exist.
func loadOutsideTree(storage store.Store, id int64, cache map[int64]*model.Workflow) (*model.Workflow, error) {
	if workflow, ok := cache[id]; ok {
		return workflow, nil
	}
	workflow, err := storage.WorkflowLoad(id)
	switch {
	case errors.Is(err, store_types.ErrRecordNotExist):
		workflow = nil
	case err != nil:
		return nil, fmt.Errorf("load workflow %d: %w", id, err)
	}
	cache[id] = workflow
	return workflow, nil
}

// withMissingDeps adds an error-state stand-in for each dependency proven deleted from the store
// and returns the IDs of those stand-ins.
func withMissingDeps(storage store.Store, task *model.Task, workflows []*model.Workflow, cache map[int64]*model.Workflow) ([]*model.Workflow, map[int64]bool) {
	extended := workflows
	var deleted map[int64]bool
	for _, dep := range task.Dependencies {
		if slices.ContainsFunc(workflows, func(workflow *model.Workflow) bool { return fmt.Sprint(workflow.ID) == dep }) {
			continue
		}
		id, err := strconv.ParseInt(dep, 10, 64)
		if err != nil {
			log.Error().Err(err).Str("task_id", task.ID).Str("dependency", dep).Msg("invalid dependency workflow id")
			continue
		}
		workflow, err := loadOutsideTree(storage, id, cache)
		if err != nil {
			log.Error().Err(err).Str("task_id", task.ID).Msg("could not check dependency outside the pipeline tree")
			continue
		}
		if workflow != nil {
			continue
		}
		if deleted == nil {
			deleted = make(map[int64]bool)
		}
		deleted[id] = true
		extended = append(slices.Clip(extended), &model.Workflow{ID: id, State: model.StatusError})
	}
	return extended, deleted
}

// finalizeDeadWorkflows ends queued workflows whose finished dependencies forbid them to run.
// A dependent of a deleted dependency ends error so later passes and the forge see the failure; others end skipped.
func finalizeDeadWorkflows(ctx context.Context, storage store.Store, workflows []*model.Workflow, waiting map[string]*model.Task, now int64, cache map[int64]*model.Workflow) []*model.Workflow {
	var finalized []*model.Workflow
	failed := make(map[int64]struct{})
	for found := true; found; {
		found = false
		for _, workflow := range workflows {
			id := fmt.Sprint(workflow.ID)
			task, queued := waiting[id]
			if _, skip := failed[workflow.ID]; skip || !queued || workflow.State != model.StatusPending {
				continue
			}
			resolved, deleted := withMissingDeps(storage, task, workflows, cache)
			deps, ok := treeDepStatus(task, resolved)
			if !ok || (&model.Task{RunOn: task.RunOn, DepStatus: deps}).ShouldRun() {
				continue
			}
			reason := "dependency-failed"
			if len(deleted) > 0 {
				reason = "dependency-deleted"
				if !reapWorkflow(storage, workflow, now, model.StatusError, deletedDependencyReason(deleted)) {
					failed[workflow.ID] = struct{}{}
					continue
				}
			} else if !skipDeadWorkflow(storage, workflow, now) {
				failed[workflow.ID] = struct{}{}
				continue
			}
			if err := server.Config.Services.Scheduler.CancelWorkflows(ctx, []string{id}); err != nil {
				log.Error().Err(err).Str("task_id", id).Msg("could not remove finalized workflow from queue")
			}
			log.Info().
				Int64("workflow_id", workflow.ID).
				Str("reason", reason).
				Strs("labels", reasonLabels(task)).
				Msg("finalized queued workflow")
			finalized = append(finalized, workflow)
			found = true
		}
	}
	return finalized
}

// deletedDependencyReason names the lowest deleted dependency ID.
func deletedDependencyReason(deleted map[int64]bool) string {
	return fmt.Sprintf("dependency workflow %d no longer exists", slices.Min(slices.Collect(maps.Keys(deleted))))
}

// skipDeadWorkflow writes the pending steps then the workflow as skipped; a failed write leaves it pending.
func skipDeadWorkflow(storage store.Store, workflow *model.Workflow, now int64) bool {
	if !skipPendingSteps(storage, workflow) {
		return false
	}
	updated := *workflow
	updated.Finished = now
	skipped, err := UpdateWorkflowToStatusSkipped(storage, updated)
	if err != nil {
		log.Error().Err(err).Int64("workflow_id", workflow.ID).Msg("could not skip workflow whose dependencies failed")
		return false
	}
	*workflow = *skipped
	return true
}

// skipPendingSteps cancels each pending step and reports whether every write succeeded.
func skipPendingSteps(storage store.Store, workflow *model.Workflow) bool {
	for _, step := range workflow.Children {
		if step.State != model.StatusPending {
			continue
		}
		skipped, err := UpdateStepToStatusSkipped(storage, *step, 0, model.StatusCanceled)
		if err != nil {
			log.Error().Err(err).Int64("step_id", step.ID).Msg("could not skip step whose dependencies failed")
			return false
		}
		*step = *skipped
	}
	return true
}

// removeStaleTaskRows drops persisted tasks whose workflow is terminal or gone.
func removeStaleTaskRows(ctx context.Context, storage store.Store, rows []*model.Task, loaded map[string]model.StatusValue, waiting map[string]*model.Task, running map[string]struct{}, cache map[int64]*model.Workflow) {
	for _, row := range rows {
		if _, leased := running[row.ID]; leased {
			continue
		}
		state, ok := loaded[row.ID]
		var missingErr error
		if !ok {
			id, err := strconv.ParseInt(row.ID, 10, 64)
			if err != nil {
				log.Error().Err(err).Str("task_id", row.ID).Msg("invalid workflow id of persisted task")
				continue
			}
			workflow, err := loadOutsideTree(storage, id, cache)
			switch {
			case err != nil:
				log.Error().Err(err).Str("task_id", row.ID).Msg("could not load workflow of persisted task")
				continue
			case workflow == nil:
				missingErr = store_types.ErrRecordNotExist
			default:
				state = workflow.State
			}
		}
		if missingErr == nil && !model.IsTerminalStatus(state) && state != model.StatusSkipped {
			continue
		}

		var err error
		_, queued := waiting[row.ID]
		switch {
		case queued && missingErr != nil:
			err = server.Config.Services.Scheduler.Error(ctx, row.ID, missingErr)
		case queued:
			err = server.Config.Services.Scheduler.Done(ctx, row.ID, state)
		default:
			if err = storage.TaskDelete(row.ID); errors.Is(err, store_types.ErrRecordNotExist) {
				err = nil
			}
		}
		if err != nil {
			log.Error().Err(err).Str("task_id", row.ID).Msg("could not remove stale persisted task")
			continue
		}
		log.Info().Str("task_id", row.ID).Str("state", string(state)).Msg("removed stale persisted task")
	}
}
