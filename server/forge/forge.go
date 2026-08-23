// Copyright 2022 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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

// Package forge defines the Forge interface for integrating with Git hosting
// platforms (GitHub, GitLab, Gitea, Forgejo, Bitbucket, etc.).
//
// The Forge interface provides a unified abstraction for OAuth authentication,
// repository management, webhook processing, and status reporting.
package forge

import (
	"context"
	"net/http"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// Forge defines the interface for integrating with Git hosting platforms.
//
// Architecture:
// A Forge instance represents a single forge provider. Woodpecker supports
// multiple forge instances simultaneously through ForgeManager.
// Each User and Repo has a ForgeID field associating them with a specific forge.
//
// Thread Safety:
// Implementations must be safe for concurrent use. Methods receive context.Context
// for cancellation/timeout. Do not maintain user-specific state; user context is
// passed via *model.User parameter.
//
// Authentication:
// OAuth2-based authentication is assumed. Tokens are refreshed 30 minutes before
// expiry via the optional Refresher interface.
//
// Configuration Fetching:
// Pipeline configurations retrieved via File() or Dir() from Repo.Config path
// with fallback to defaults.
//
// Error Handling:
// - types.ErrIgnoreEvent: Skippable webhook events
// - types.ErrRecordNotExist: Resource not found
// - types.ErrNotImplemented: Can be used to signal it's not supported
// - nil Repo/Pipeline: "No action needed" (not an error).
type Forge interface {
	// Name returns the unique identifier of this forge driver.
	// Examples: "github", "gitlab", "gitea", "forgejo", "bitbucket"
	// Must be unique and constant across all implementations.
	Name() string

	// URL returns the root URL of the forge instance.
	// Examples: "https://github.com", "https://gitlab.example.com"
	URL() string

	// Login authenticates a user via OAuth2.
	//
	// OAuth Flow:
	//  1. Initial call with empty OAuthRequest.Code returns (nil, redirectURL, nil)
	//  2. User authorizes at redirectURL
	//  3. Second call with OAuthRequest.Code returns (User, redirectURL, nil)
	//
	// Returned User must contain: Login, Email, Avatar, AccessToken, RefreshToken, Expiry, ForgeRemoteID
	Login(ctx context.Context, r *types.OAuthRequest) (*model.User, string, error)

	// Teams fetches all team/organization memberships for a user.
	// Used to determine if an user is member of an team/organization.
	// Should support pagination via ListOptions.
	//
	// Errors:
	//  - Expect types.ErrNotImplemented to be returned if forge doesn't support teams/organizations.
	Teams(ctx context.Context, u *model.User, p *model.ListOptions) ([]*model.Team, error)

	// Repo fetches a single repository.
	//
	// Lookup Strategy:
	// - Prefer lookup by remoteID (forge's internal ID) if provided (more reliable as repos can be renamed)
	// - Fallback to owner/name if remoteID empty
	//
	// Must verify user has at least read access.
	// Caller must make sure ForgeID is set.
	Repo(ctx context.Context, u *model.User, remoteID model.ForgeRemoteID, owner, name string) (*model.Repo, error)

	// Repos fetches all repositories accessible to the user.
	// Should include user's permission level in Repo.Perm.
	// Should support pagination via ListOptions.
	// Caller must make sure ForgeID is set.
	Repos(ctx context.Context, u *model.User, p *model.ListOptions) ([]*model.Repo, error)

	// File fetches a single file at a specific commit.
	// Primary method for retrieving pipeline configuration files.
	// Must fetch at specific commit (b.Commit), not branch head.
	File(ctx context.Context, u *model.User, r *model.Repo, b *model.Pipeline, fileName string) ([]byte, error)

	// Dir fetches all files in a directory at a specific commit.
	// Supports pipeline configurations split across multiple files.
	// Should return files only.
	//
	// Errors:
	//  - Expect types.ErrNotImplemented to be returned if not supported by the forge
	Dir(ctx context.Context, u *model.User, r *model.Repo, b *model.Pipeline, dirName string) ([]*types.FileMeta, error)

	// Status sends workflow status updates to the forge.
	// Provides visual feedback in forge UI (commit checks, PR status).
	// Failures should be logged but not block pipeline execution.
	Status(ctx context.Context, u *model.User, r *model.Repo, b *model.Pipeline, p *model.Workflow) error

	// Netrc generates .netrc credentials for cloning private repositories.
	// May receive nil user for public repos.
	Netrc(u *model.User, r *model.Repo) (*model.Netrc, error)

	// Activate creates a webhook pointing to Woodpecker.
	// Called when user activates a repository.
	// Must verify user has admin access. Should set webhook secret from r.Hash.
	// Configure webhook for all events Hook() can parse.
	Activate(ctx context.Context, u *model.User, r *model.Repo, link string) error

	// Deactivate removes the webhook.
	// Should ignore if webhook doesn't exist anymore.
	Deactivate(ctx context.Context, u *model.User, r *model.Repo, link string) error

	// Branches returns all branch names in the repository.
	// Should support pagination via ListOptions.
	//
	// Errors:
	//  - Expect types.ErrNotImplemented to be returned if not supported by the forge
	Branches(ctx context.Context, u *model.User, r *model.Repo, p *model.ListOptions) ([]string, error)

	// BranchHead returns the latest commit SHA for a branch.
	// Is essential for cron feature to work.
	BranchHead(ctx context.Context, u *model.User, r *model.Repo, branch string) (*model.Commit, error)

	// PullRequests returns all open pull requests.
	// Should support pagination via ListOptions.
	//
	// Errors:
	//  - Expect types.ErrNotImplemented to be returned if not supported by the forge
	PullRequests(ctx context.Context, u *model.User, r *model.Repo, p *model.ListOptions) ([]*model.PullRequest, error)

	// Hook parses incoming webhook and returns pipeline data.
	//
	// Webhook Processing Flow:
	//  1. HTTP request arrives at /api/hook with forge-specific format
	//  2. Webhook token verified against repo.Hash
	//  3. Hook() parses webhook and returns (Repo, Pipeline, error)
	//
	// Return Semantics:
	// - (repo, pipeline, nil): Execute pipeline for this event
	// - (repo, nil, nil): Valid webhook, no pipeline should run
	// - (nil, nil, types.ErrIgnoreEvent): Event ignored (logged)
	// - (nil, nil, error): Invalid webhook or parsing error
	//
	// Must verify webhook signature to prevent spoofing.
	// Should return types.ErrIgnoreEvent for non-pipeline events
	// (e.g. repository settings changed).
	Hook(ctx context.Context, r *http.Request) (*model.Repo, *model.Pipeline, error)

	// OrgMembership checks if user is member of organization and their permission.
	// Should return (Member: false, Admin: false) if not a member.
	//
	// Errors:
	//  - Expect types.ErrNotImplemented to be returned if not supported by the forge
	OrgMembership(ctx context.Context, u *model.User, org string) (*model.OrgPerm, error)

	// Org fetches organization details.
	// If identifier is a user, return org with IsUser: true.
	Org(ctx context.Context, u *model.User, org string) (*model.Org, error)
}

// AggregateStatusReporter is an optional Forge capability: report a single
// pipeline-level status that rolls up the CODE (non-meta) workflows' state.
// Because it has no per-workflow component, it is stable across an
// affected-aware fan-out and can serve as a required branch-protection check
// that only passes when the whole code pipeline passes. It receives the workflow
// tree so it can exclude the meta gates (those go to the sibling
// MetaStatusReporter's CI (meta) context), keeping CI (pr) from redding on a
// gate a title/body edit can fix.
type AggregateStatusReporter interface {
	StatusAggregate(ctx context.Context, u *model.User, r *model.Repo, b *model.Pipeline, workflows []*model.Workflow) error
}

// ReportAggregateStatus reports the pipeline-level code aggregate status when the
// forge supports it; it is a no-op for forges that don't. It self-loads the
// workflow tree the reporter needs to partition the rollup:
//
//  1. Event scope — only pull_request and pull_request_metadata pipelines carry
//     the meta gates, so only they need the tree to exclude them. Every other
//     event (push, tag, cron, deploy) has no meta gate, so its stored pipeline
//     status is already the code verdict — pass no tree and let StatusAggregate
//     fall back to it, adding no store read on the hot push path.
//  2. Workflow self-load — updatePipelineStatus can run BEFORE the tree is loaded
//     (the cancel path loads it after), so this loads it itself rather than
//     trusting caller load order; otherwise the partition silently falls back to
//     the whole-pipeline status and re-counts the meta gates into CI (pr). It
//     populates b.Workflows so the sibling ReportMetaStatus (called next) reuses
//     this one read instead of loading the tree a second time.
func ReportAggregateStatus(ctx context.Context, f Forge, s store.Store, u *model.User, r *model.Repo, b *model.Pipeline) error {
	reporter, ok := f.(AggregateStatusReporter)
	if !ok {
		return nil
	}

	var workflows []*model.Workflow
	if b.Event == model.EventPull || b.Event == model.EventPullMetadata {
		workflows = b.Workflows
		if len(workflows) == 0 {
			var err error
			workflows, err = s.WorkflowGetTree(b)
			if err != nil {
				return err
			}
			b.Workflows = workflows
		}
	}

	return reporter.StatusAggregate(ctx, u, r, b, workflows)
}

// MetaStatusReporter is an optional Forge capability: report a SECOND, selective
// aggregate status that rolls up ONLY the configured meta-gate workflows under a
// stable, event-independent context. It is a sibling of AggregateStatusReporter,
// so a cheap metadata-only pipeline can re-post the meta verdict without ever
// masking the code aggregate.
type MetaStatusReporter interface {
	StatusMeta(ctx context.Context, u *model.User, r *model.Repo, b *model.Pipeline, workflows []*model.Workflow) error
}

// ReportMetaStatus reports the selective meta-aggregate status when the forge
// supports it; it is a no-op for forges that don't. It centralizes three
// concerns so every updatePipelineStatus caller inherits them without a
// per-call-site audit:
//
//  1. Event scope — only pull_request and pull_request_metadata pipelines carry
//     the meta gates, so only they report the meta status.
//  2. Workflow self-load — the meta poster filters workflows, so it needs the
//     tree; unlike the code aggregate it cannot tolerate a nil pipeline.Workflows.
//     updatePipelineStatus runs at cancel.go BEFORE the tree is loaded, so this
//     loads it itself rather than trusting caller load order — otherwise a
//     canceled pipeline no-ops the post and strands the required check pending.
//  3. Freshness — CI (meta) has two writers by design (the pull_request pipeline
//     and every pull_request_metadata pipeline for the same commit) and nothing
//     orders their completion. To stop a slow older pipeline from re-posting its
//     stale verdict over a fresher one (in the worst case BYPASSING the gate:
//     open-green → edit-bad → the slow pull pipeline re-greens), skip when the
//     store already holds a LATER meta-carrying pipeline (higher Number) for the
//     same commit + PR. Meta-carrying is the EVENT, so this needs only the
//     pipeline rows (event + ref-scoped), never a per-candidate workflow-tree scan.
func ReportMetaStatus(ctx context.Context, f Forge, s store.Store, u *model.User, r *model.Repo, b *model.Pipeline) error {
	reporter, ok := f.(MetaStatusReporter)
	if !ok {
		return nil
	}

	// (1) Event scope: only PR-family pipelines carry the meta gates.
	if b.Event != model.EventPull && b.Event != model.EventPullMetadata {
		return nil
	}

	// (3) Freshness: skip if a later meta-carrying pipeline exists for this
	// commit + PR, so only the newest meta-bearing pipeline's verdict survives.
	newer, err := hasLaterMetaPipeline(s, r, b)
	if err != nil {
		return err
	}
	if newer {
		return nil
	}

	// (2) Self-load the workflow tree when the caller has not loaded it (e.g. the
	// cancel path), so filtering never no-ops on a nil tree and strands the check.
	workflows := b.Workflows
	if len(workflows) == 0 {
		workflows, err = s.WorkflowGetTree(b)
		if err != nil {
			return err
		}
	}

	return reporter.StatusMeta(ctx, u, r, b, workflows)
}

// hasLaterMetaPipeline reports whether the store holds a meta-carrying pipeline
// (a pull_request or pull_request_metadata event) for the same commit + PR with a
// higher pipeline Number than b. It reads only pipeline rows: the query is
// event-filtered and ref-scoped (the PR ref), and the head commit is matched in
// memory — no candidate's workflow tree is ever loaded.
func hasLaterMetaPipeline(s store.Store, r *model.Repo, b *model.Pipeline) (bool, error) {
	pipelines, err := s.GetPipelineList(r, &model.ListOptionsWithAll{All: true}, &model.PipelineFilter{
		Events:      []model.WebhookEvent{model.EventPull, model.EventPullMetadata},
		RefContains: b.Ref,
	})
	if err != nil {
		return false, err
	}
	for _, p := range pipelines {
		// A newer pipeline that errored before its workflows were persisted (e.g. a
		// config-fetch failure) never posts a meta verdict, so it must not suppress
		// this pipeline's terminal report -- otherwise the required CI (meta) check
		// is left stranded. Only defer to a newer pipeline that will actually post.
		if p.Status == model.StatusError {
			continue
		}
		if p.Commit == b.Commit && p.Number > b.Number {
			return true, nil
		}
	}
	return false, nil
}
