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

	"github.com/google/go-github/v90/github"

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
	// workflows, so the filtered rollup is the live path there.)
	status := p.Status
	code := make([]*model.Workflow, 0, len(workflows))
	for _, workflow := range workflows {
		if !workflow.OnMetadataEdit {
			code = append(code, workflow)
		}
	}
	if len(code) > 0 {
		status = pipeline.PipelineStatus(code)
		// Terminal-never-pending: a cancel sets a terminal pipeline status but
		// leaves the still-running workflows untouched (they finish on the agent's
		// stop signal — cancel.go), so the filtered rollup can be StatusRunning
		// (→ pending) while the pipeline is already StatusKilled. The required
		// check must reflect the terminal pipeline verdict, never a stale pending
		// that could strand the merge gate if the agent never reports Done.
		status = reconcileTerminalStatus(status, p.Status)
	}

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
		_, resp, e := client.Repositories.CreateStatus(ctx, repo.Owner, repo.Name, p.Commit, github.RepoStatus{
			Context:     github.Ptr(common.GetPipelineAggregateStatusContext(repo, p)),
			State:       github.Ptr(convertStatus(status)),
			Description: github.Ptr(common.GetPipelineStatusDescription(status)),
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
	// Filter to the meta gates: workflows whose `when` listens on the
	// pull_request_metadata event, persisted at build time. Matching nothing
	// means this pipeline carries no meta gate, so there is nothing to report.
	matched := make([]*model.Workflow, 0, len(workflows))
	for _, workflow := range workflows {
		if workflow.OnMetadataEdit {
			matched = append(matched, workflow)
		}
	}
	if len(matched) == 0 {
		return nil
	}

	// Roll up ONLY the matched workflows, reusing the same state-merge the code
	// aggregate uses over its full set. This is the meta verdict. Reconcile
	// against the terminal pipeline status for the same cancel-while-running
	// reason as StatusAggregate: a canceled pipeline can leave a meta gate
	// StatusRunning (→ pending) while p.Status is already terminal, and this is a
	// required check that must never strand pending.
	metaStatus := reconcileTerminalStatus(pipeline.PipelineStatus(matched), p.Status)

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
		_, resp, e := client.Repositories.CreateStatus(ctx, repo.Owner, repo.Name, p.Commit, github.RepoStatus{
			Context:     github.Ptr(common.GetPipelineMetaStatusContext(repo, p)),
			State:       github.Ptr(convertStatus(metaStatus)),
			Description: github.Ptr(common.GetPipelineStatusDescription(metaStatus)),
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
// naive rollup would post "pending" to a required branch-protection check, which
// flaps the gate and — if the agent never reports Done — strands it pending
// forever. When the rolled-up status is non-terminal but the pipeline status is
// terminal, prefer the pipeline status; otherwise keep the (more specific)
// rolled-up verdict.
func reconcileTerminalStatus(rolled, pipelineStatus model.StatusValue) model.StatusValue {
	if isTerminalStatus(rolled) || !isTerminalStatus(pipelineStatus) {
		return rolled
	}
	return pipelineStatus
}

// isTerminalStatus reports whether a status is a final pipeline verdict — one
// that convertStatus maps to a terminal GitHub commit state (success/failure)
// rather than pending. StatusBlocked (awaiting approval) and StatusCreated are
// deliberately NOT terminal: they are legitimately still pending.
func isTerminalStatus(s model.StatusValue) bool {
	switch s {
	case model.StatusSuccess, model.StatusFailure, model.StatusKilled,
		model.StatusError, model.StatusDeclined, model.StatusCanceled:
		return true
	default:
		return false
	}
}
