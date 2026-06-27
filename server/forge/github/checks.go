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
	"strconv"

	"github.com/google/go-github/v88/github"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/common"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	checkRunStatusQueued     = "queued"
	checkRunStatusInProgress = "in_progress"
	checkRunStatusCompleted  = "completed"
)

const (
	checkRunConclusionSuccess   = "success"
	checkRunConclusionFailure   = "failure"
	checkRunConclusionCancelled = "cancelled" //nolint:misspell // literal GitHub Checks API conclusion value
	checkRunConclusionSkipped   = "skipped"
)

// checkRunStatus maps a Woodpecker workflow state to a GitHub check-run status.
func checkRunStatus(status model.StatusValue) string {
	switch status {
	case model.StatusPending, model.StatusBlocked, model.StatusCreated:
		return checkRunStatusQueued
	case model.StatusRunning:
		return checkRunStatusInProgress
	default:
		return checkRunStatusCompleted
	}
}

// checkRunConclusion maps a Woodpecker workflow state to a GitHub check-run
// conclusion. It returns an empty string for states that are not yet completed.
func checkRunConclusion(status model.StatusValue) string {
	switch status {
	case model.StatusSuccess:
		return checkRunConclusionSuccess
	case model.StatusFailure, model.StatusError:
		return checkRunConclusionFailure
	case model.StatusKilled, model.StatusCanceled, model.StatusDeclined:
		return checkRunConclusionCancelled
	case model.StatusSkipped:
		return checkRunConclusionSkipped
	default:
		return ""
	}
}

// createOrUpdateCheckRun reports a workflow as a GitHub check-run. Repeated
// updates for the same workflow reuse a single check-run (matched by the
// workflow ID stored as ExternalID) instead of creating duplicates.
func (c *client) createOrUpdateCheckRun(ctx context.Context, gh *github.Client, repo *model.Repo, pipeline *model.Pipeline, workflow *model.Workflow) error {
	externalID := strconv.FormatInt(workflow.ID, 10)
	name := common.GetPipelineStatusContext(repo, pipeline, workflow)
	detailsURL := common.GetPipelineStatusURL(repo, pipeline, workflow)
	status := checkRunStatus(workflow.State)

	output := &github.CheckRunOutput{
		Title:   github.Ptr(name),
		Summary: github.Ptr(common.GetPipelineStatusDescription(workflow.State)),
	}

	existing, err := c.findCheckRun(ctx, gh, repo, pipeline.Commit, externalID)
	if err != nil {
		return err
	}

	if existing != nil {
		opts := github.UpdateCheckRunOptions{
			Name:       name,
			DetailsURL: github.Ptr(detailsURL),
			ExternalID: github.Ptr(externalID),
			Status:     github.Ptr(status),
			Output:     output,
		}
		if status == checkRunStatusCompleted {
			opts.Conclusion = github.Ptr(checkRunConclusion(workflow.State))
		}
		_, _, err = gh.Checks.UpdateCheckRun(ctx, repo.Owner, repo.Name, existing.GetID(), opts)
		return err
	}

	opts := github.CreateCheckRunOptions{
		Name:       name,
		HeadSHA:    pipeline.Commit,
		DetailsURL: github.Ptr(detailsURL),
		ExternalID: github.Ptr(externalID),
		Status:     github.Ptr(status),
		Output:     output,
	}
	if status == checkRunStatusCompleted {
		opts.Conclusion = github.Ptr(checkRunConclusion(workflow.State))
	}
	_, _, err = gh.Checks.CreateCheckRun(ctx, repo.Owner, repo.Name, opts)
	return err
}

// findCheckRun returns the check-run this app previously created for the commit
// with the given external ID, or nil if none exists.
func (c *client) findCheckRun(ctx context.Context, gh *github.Client, repo *model.Repo, sha, externalID string) (*github.CheckRun, error) {
	opts := &github.ListCheckRunsOptions{
		ListOptions: github.ListOptions{PerPage: defaultPageSize},
	}
	for {
		result, resp, err := gh.Checks.ListCheckRunsForRef(ctx, repo.Owner, repo.Name, sha, opts)
		if err != nil {
			return nil, err
		}
		for _, run := range result.CheckRuns {
			if run.GetExternalID() == externalID && run.GetApp().GetID() == c.appID {
				return run, nil
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return nil, nil
}
