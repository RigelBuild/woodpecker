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

	client, err := c.newClientToken(ctx, user.AccessToken)
	if err != nil {
		return err
	}

	_, _, err = client.Repositories.CreateStatus(ctx, repo.Owner, repo.Name, pipeline.Commit, github.RepoStatus{
		Context:     github.Ptr(common.GetPipelineAggregateStatusContext(repo, pipeline)),
		State:       github.Ptr(convertStatus(pipeline.Status)),
		Description: github.Ptr(common.GetPipelineStatusDescription(pipeline.Status)),
		TargetURL:   github.Ptr(common.GetPipelineStatusURL(repo, pipeline, nil)),
	})
	return err
}
