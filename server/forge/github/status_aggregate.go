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
	"slices"

	"github.com/google/go-github/v90/github"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/common"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// StatusAggregate reports the pipeline's overall CODE state as a single commit
// status with a stable, fan-out-independent context (CI (pr)). Unlike the
// per-workflow statuses it is always present, so it can be set as a required
// branch-protection check that only passes when the whole code pipeline passes.
// It uses the commit-status API, which works as a required check on any
// Woodpecker — no GitHub App needed.
//
// It rolls up ONLY the non-meta workflows (those that do NOT listen on
// pull_request_metadata): the meta gates get their own required CI (meta) context
// (StatusMeta), so CI (pr) must never red on a meta gate that a title/body edit
// can fix — it gates the "real" code CI alone. This is the sibling of StatusMeta,
// which rolls up ONLY the meta gates; together they partition the workflow set.
func (c *client) StatusAggregate(ctx context.Context, user *model.User, repo *model.Repo, p *model.Pipeline, workflows []*model.Workflow) error {
	// Deployments report their own deployment status; no aggregate for them.
	if p.Event == model.EventDeploy {
		return nil
	}

	// Roll up ONLY the code (non-meta) workflows. Fall back to the stored
	// pipeline status when no code workflow is present: a config-fetch-errored
	// pipeline persists no tree, and its terminal p.Status IS the correct verdict
	// — rolling up an empty set would post a vacuous success and mask the error,
	// stranding the required check green. (A real PR pipeline always carries code
	// workflows, so the filtered aggregate is the live path there.)
	status := codeStatus(p, workflows)

	// Decouple from the caller's (agent gRPC) deadline and bound the report on
	// its own budget: the aggregate status is the required branch-protection
	// check, so a slow or rate-limited commit-status POST must fail fast with
	// backoff rather than hang and leave the check stuck "pending" forever.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusReportTimeout)
	defer cancel()

	client, err := c.newClientToken(ctx, user.AccessToken)
	if err != nil {
		return err
	}

	_, err = doForgeWrite(ctx, func() (*github.Response, error) {
		state := attemptStatus(ctx, status, codeStatus)
		_, resp, e := client.Repositories.CreateStatus(ctx, repo.Owner, repo.Name, p.Commit, github.RepoStatus{
			Context:     github.Ptr(common.GetPipelineAggregateStatusContext(repo, p)),
			State:       github.Ptr(convertStatus(state)),
			Description: github.Ptr(common.GetPipelineStatusDescription(state)),
			TargetURL:   github.Ptr(common.GetPipelineStatusURL(repo, p, nil)),
		})
		return resp, e
	})
	return err
}

// StatusMeta reports a SECOND, selective aggregate status that rolls up ONLY the
// workflows that listen on the pull_request_metadata event (the "meta gates"),
// under a stable, event-independent context (GetPipelineMetaStatusContext). It
// is a sibling of StatusAggregate, not a replacement: the code aggregate (CI
// (pr)) keeps rolling up every workflow, while this meta context can be
// re-posted by a cheap metadata-only pipeline without ever masking the code
// verdict.
//
// It no-ops (posts nothing) when none of the pipeline's workflows are meta
// gates, so a pipeline that carries no meta gate never touches the context.
func (c *client) StatusMeta(ctx context.Context, user *model.User, repo *model.Repo, p *model.Pipeline, workflows []*model.Workflow) error {
	// Matching no meta gate means this pipeline has nothing to report.
	if !slices.ContainsFunc(workflows, func(w *model.Workflow) bool { return w.OnMetadataEdit }) {
		return nil
	}
	metaStatus := metaGateStatus(p, workflows)

	// Decouple from the caller's (agent gRPC) deadline and bound the report on
	// its own budget, exactly like StatusAggregate: the meta status is a required
	// branch-protection check, so a slow or rate-limited POST must fail fast with
	// backoff rather than hang and leave the check stuck "pending" forever.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusReportTimeout)
	defer cancel()

	client, err := c.newClientToken(ctx, user.AccessToken)
	if err != nil {
		return err
	}

	_, err = doForgeWrite(ctx, func() (*github.Response, error) {
		state := attemptStatus(ctx, metaStatus, metaGateStatus)
		_, resp, e := client.Repositories.CreateStatus(ctx, repo.Owner, repo.Name, p.Commit, github.RepoStatus{
			Context:     github.Ptr(common.GetPipelineMetaStatusContext(repo, p)),
			State:       github.Ptr(convertStatus(state)),
			Description: github.Ptr(common.GetPipelineStatusDescription(state)),
			TargetURL:   github.Ptr(common.GetPipelineStatusURL(repo, p, nil)),
		})
		return resp, e
	})
	return err
}

// reconcileTerminalStatus keeps a required commit status from posting a
// non-terminal state once the pipeline itself has reached a terminal one. The
// rolled-up workflow status can be StatusRunning/StatusPending when a cancel set
// a terminal pipeline status but deliberately left the still-running workflows
// untouched (cancel.go — they finish on the agent stop signal). In that window a
// naive aggregate would post "pending" to a required branch-protection check, which
// flaps the gate and — if the agent never reports Done — strands it pending
// forever. When the rolled-up status is non-terminal but the pipeline status is
// terminal, prefer the pipeline status; otherwise keep the (more specific)
// rolled-up verdict.
func reconcileTerminalStatus(rolled, pipelineStatus model.StatusValue) model.StatusValue {
	if model.IsTerminalStatus(rolled) || !model.IsTerminalStatus(pipelineStatus) {
		return rolled
	}
	return pipelineStatus
}

// codeStatus rolls up the code (non-meta) workflows. With none it falls back to
// the pipeline status: a config-errored pipeline persists no tree, and rolling up
// an empty set would post a vacuous success.
func codeStatus(p *model.Pipeline, workflows []*model.Workflow) model.StatusValue {
	code := make([]*model.Workflow, 0, len(workflows))
	for _, workflow := range workflows {
		if !workflow.OnMetadataEdit {
			code = append(code, workflow)
		}
	}
	if len(code) == 0 {
		return p.Status
	}
	return reconcileTerminalStatus(pipeline.PipelineStatus(code), p.Status)
}

// metaGateStatus rolls up only the meta-gate workflows.
func metaGateStatus(p *model.Pipeline, workflows []*model.Workflow) model.StatusValue {
	matched := make([]*model.Workflow, 0, len(workflows))
	for _, workflow := range workflows {
		if workflow.OnMetadataEdit {
			matched = append(matched, workflow)
		}
	}
	return reconcileTerminalStatus(pipeline.PipelineStatus(matched), p.Status)
}

// attemptStatus re-reads the pipeline before each write attempt. Reports run
// unordered and may sit in backoff, so a pending verdict from an older snapshot
// is recomputed once the pipeline has finished.
func attemptStatus(ctx context.Context, status model.StatusValue,
	rollup func(*model.Pipeline, []*model.Workflow) model.StatusValue,
) model.StatusValue {
	refresh := forge.RefresherFromContext(ctx)
	if model.IsTerminalStatus(status) || refresh == nil {
		return status
	}
	if stored, tree, ok := refresh(); ok {
		return rollup(stored, tree)
	}
	return status
}
