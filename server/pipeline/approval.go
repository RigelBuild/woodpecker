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
	"fmt"
	"slices"
	"strings"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// Trunk merge-queue branches: the queue's merge branches and its trial branches.
var trunkMergeBranchPrefixes = []string{"trunk-merge/", "trunk-temp/"}

// Approval is an explicit approval of a blocked pipeline. Only Approve builds one;
// Create and Restart pass nil, so their runs never receive a gated secret.
type Approval struct {
	Login string
	// Session is true only for a CSRF-checked browser session, never an API token.
	Session bool
}

// secretReleased reports whether an approval releases gated secrets. An empty
// approver list, a nil approval or a non-session approval releases nothing.
func secretReleased(a *Approval, approvers []string) bool {
	if a == nil || !a.Session || a.Login == "" {
		return false
	}
	return slices.Contains(approvers, a.Login)
}

// isTrunkMergeRun reports whether the pipeline runs on a merge-queue branch, where
// PR-controlled config runs and gated secrets must never be released. A
// pull_request run names its head branch only in the Refspec.
func isTrunkMergeRun(p *model.Pipeline) bool {
	head, _, _ := strings.Cut(p.Refspec, ":")
	for _, branch := range []string{p.Branch, strings.TrimPrefix(p.Ref, "refs/heads/"), head} {
		for _, prefix := range trunkMergeBranchPrefixes {
			if strings.HasPrefix(branch, prefix) {
				return true
			}
		}
	}
	return false
}

// branchProof decides whether a run's Branch can be trusted for branch-filtered
// secrets. A deployment may name any branch for any commit, so only a commit at
// an allowed branch's current head counts; a restart that changed the event
// cannot prove anything.
type branchProof struct {
	ctx   context.Context
	forge forge.Forge
	user  *model.User
	repo  *model.Repo
	p     *model.Pipeline
	heads map[string]string
}

// secretBranchAllowed applies a secret's branch filter. Pull request runs are left
// to the event filter, as the branch there is the PR target, not the code's source.
func (b *branchProof) secretBranchAllowed(sec *model.Secret) (bool, error) {
	if len(sec.Branches) == 0 || b.p.IsPullRequest() {
		return true, nil
	}
	if b.p.EventOverridden {
		return false, nil
	}
	if b.p.Event != model.EventDeploy {
		return slices.Contains(sec.Branches, b.p.Branch), nil
	}
	for _, branch := range sec.Branches {
		head, err := b.branchHead(branch)
		if err != nil {
			return false, err
		}
		if head != "" && head == b.p.Commit {
			return true, nil
		}
	}
	return false, nil
}

// branchHead looks up a branch's head once per parse. A failed lookup fails the
// parse, so an unreachable forge never releases a branch-filtered secret.
func (b *branchProof) branchHead(branch string) (string, error) {
	if head, ok := b.heads[branch]; ok {
		return head, nil
	}
	commit, err := b.forge.BranchHead(b.ctx, b.user, b.repo, branch)
	if err != nil {
		return "", fmt.Errorf("failed to read the head of branch %q: %w", branch, err)
	}
	if b.heads == nil {
		b.heads = map[string]string{}
	}
	b.heads[branch] = commit.SHA
	return commit.SHA, nil
}

// secretEventAllowed mirrors the compiler's event filter, so a secret the run's
// event can never use is not reported as waiting for approval.
func secretEventAllowed(sec *model.Secret, p *model.Pipeline) bool {
	if len(sec.Events) == 0 {
		return true
	}
	event := p.Event
	if p.IsPullRequest() {
		event = model.EventPull
	}
	return slices.Contains(sec.Events, event)
}

// hasCallerVariables reports whether the run injects caller-chosen variables into
// every step, where one such as BASH_ENV runs the caller's code next to the secrets.
func hasCallerVariables(p *model.Pipeline, envs map[string]string) bool {
	return len(p.AdditionalVariables) > 0 || len(envs) > 0
}

// blockForApproval parks a pipeline whose only blocking errors are withheld
// secrets, so an approver can release it through Approve.
func blockForApproval(ctx context.Context, forge forge.Forge, store store.Store, pipeline *model.Pipeline, repo *model.Repo, user *model.User) error {
	pipeline.Status = model.StatusBlocked
	if err := store.UpdatePipeline(pipeline); err != nil {
		return err
	}
	publishPipeline(ctx, forge, store, pipeline, repo, user)
	return nil
}
