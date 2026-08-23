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

	"github.com/google/go-github/v88/github"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/common"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// StatusAggregate reports the pipeline's overall (rolled-up) state as a single
// commit status with a stable, fan-out-independent context. Unlike the
// per-workflow statuses it is always present, so it can be set as a required
// branch-protection check that only passes when the whole pipeline (every
// workflow) passes. It uses the commit-status API, which works as a required
// check on any Woodpecker — no GitHub App needed.
func (c *client) StatusAggregate(ctx context.Context, user *model.User, repo *model.Repo, pipeline *model.Pipeline) error {
	// Deployments report their own deployment status; no aggregate for them.
	if pipeline.Event == model.EventDeploy {
		return nil
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
		_, resp, e := client.Repositories.CreateStatus(ctx, repo.Owner, repo.Name, pipeline.Commit, github.RepoStatus{
			Context:     github.Ptr(common.GetPipelineAggregateStatusContext(repo, pipeline)),
			State:       github.Ptr(convertStatus(pipeline.Status)),
			Description: github.Ptr(common.GetPipelineStatusDescription(pipeline.Status)),
			TargetURL:   github.Ptr(common.GetPipelineStatusURL(repo, pipeline, nil)),
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
	// aggregate uses over its full set. This is the meta verdict.
	metaStatus := pipeline.PipelineStatus(matched)

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
