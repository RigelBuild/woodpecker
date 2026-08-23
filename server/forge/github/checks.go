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
	"net/http"
	"strconv"

	"github.com/google/go-github/v90/github"

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
// updates for the same workflow reuse a single check-run — resolved from an
// in-memory cache (commit SHA + workflow external ID) so updates call
// UpdateCheckRun directly instead of paginating ListCheckRunsForRef on every
// transition, which is the call amplification that blows the forge timeout
// under a merge-queue burst. A terminal (completed) check-run is never
// downgraded by a later out-of-order status.
func (c *client) createOrUpdateCheckRun(ctx context.Context, gh *github.Client, repo *model.Repo, pipeline *model.Pipeline, workflow *model.Workflow) error {
	externalID := strconv.FormatInt(workflow.ID, 10)
	name := common.GetPipelineStatusContext(repo, pipeline, workflow)
	detailsURL := common.GetPipelineStatusURL(repo, pipeline, workflow)
	status := checkRunStatus(workflow.State)
	key := checkRunCacheKey(pipeline.Commit, externalID)

	// Resolve the existing check-run: prefer the cache; fall back to a one-time
	// lookup that repopulates it after a restart.
	runID, lastStatus, found := c.cachedCheckRun(key)
	if !found {
		existing, err := c.findCheckRun(ctx, gh, repo, pipeline.Commit, externalID)
		if err != nil {
			return err
		}
		if existing != nil {
			runID, lastStatus, found = existing.GetID(), existing.GetStatus(), true
			c.storeCheckRun(key, checkRunRef{id: runID, status: lastStatus})
		}
	}

	// State precedence: a completed check-run is terminal. Never let a late or
	// out-of-order update (e.g. a retried "running") downgrade a reported
	// success/failure back to in-progress.
	if found && lastStatus == checkRunStatusCompleted && status != checkRunStatusCompleted {
		return nil
	}

	output := &github.CheckRunOutput{
		Title:   github.Ptr(name),
		Summary: github.Ptr(common.GetPipelineStatusDescription(workflow.State)),
	}

	if found {
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
		resp, err := doForgeWrite(ctx, func() (*github.Response, error) {
			_, r, e := gh.Checks.UpdateCheckRun(ctx, repo.Owner, repo.Name, runID, opts)
			return r, e
		})
		if err != nil {
			// A cached run ID can go stale (deleted, or a re-run replaced it).
			// Drop it and fall through to recreate; propagate any other error.
			if resp != nil && resp.StatusCode == http.StatusNotFound {
				c.dropCheckRun(key)
			} else {
				return err
			}
		} else {
			c.storeCheckRun(key, checkRunRef{id: runID, status: status})
			return nil
		}
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
	var run *github.CheckRun
	_, err := doForgeWrite(ctx, func() (*github.Response, error) {
		r, resp, e := gh.Checks.CreateCheckRun(ctx, repo.Owner, repo.Name, opts)
		run = r
		return resp, e
	})
	if err != nil {
		return err
	}
	c.storeCheckRun(key, checkRunRef{id: run.GetID(), status: status})
	return nil
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

// checkRunRef is the cached identity and last-reported status of a GitHub
// check-run, keyed by commit SHA + workflow external ID so repeated status
// transitions for one workflow reuse the same run without re-listing.
type checkRunRef struct {
	id     int64
	status string
}

func checkRunCacheKey(sha, externalID string) string {
	return sha + "/" + externalID
}

func (c *client) cachedCheckRun(key string) (id int64, status string, ok bool) {
	c.checkRunMu.Lock()
	defer c.checkRunMu.Unlock()
	ref, ok := c.checkRuns[key]
	return ref.id, ref.status, ok
}

func (c *client) storeCheckRun(key string, ref checkRunRef) {
	c.checkRunMu.Lock()
	defer c.checkRunMu.Unlock()
	if c.checkRuns == nil {
		c.checkRuns = make(map[string]checkRunRef)
	}
	c.checkRuns[key] = ref
}

func (c *client) dropCheckRun(key string) {
	c.checkRunMu.Lock()
	defer c.checkRunMu.Unlock()
	delete(c.checkRuns, key)
}
