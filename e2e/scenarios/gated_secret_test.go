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

//go:build test

package scenarios

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/cron"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

const gatedSecretName = "PULUMI_GATED_ACCESS_TOKEN"

var gatedSecretYAML = []byte(`
steps:
  - name: deploy
    image: dummy
    environment:
      TOKEN:
        from_secret: ` + gatedSecretName + `
    commands:
      - echo deploy
`)

// startGatedSecretEnv starts a server whose only workflow requests a gated secret,
// with the repo owner as the sole approver.
func startGatedSecretEnv(t *testing.T) *setup.ServerEnv {
	t.Helper()
	return startGatedSecretEnvWith(t, gatedSecretYAML)
}

// startGatedSecretEnvWith is startGatedSecretEnv with a caller-chosen workflow.
func startGatedSecretEnvWith(t *testing.T, workflow []byte) *setup.ServerEnv {
	t.Helper()
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: workflow},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	server.Config.Pipeline.SecretApprovers = []string{env.Fixtures.Owner.Login}
	require.NoError(t, env.Store.SecretCreate(&model.Secret{
		RepoID:          env.Fixtures.Repo.ID,
		Name:            gatedSecretName,
		Value:           "gated-pulumi-token-value",
		RequireApproval: true,
	}), "seed gated secret")
	return env
}

// approvalErrorNames decodes the pipeline API JSON and returns the names of
// secrets reported as withheld.
func approvalErrorNames(t *testing.T, p *model.Pipeline) []string {
	t.Helper()
	raw, err := json.Marshal(p.ToAPIModel())
	require.NoError(t, err)
	var body struct {
		Errors []struct {
			Type string `json:"type"`
			Data struct {
				Name string `json:"name"`
			} `json:"data"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	var names []string
	for _, e := range body.Errors {
		if e.Type == "approval" {
			names = append(names, e.Data.Name)
		}
	}
	return names
}

func blockedGatedPipeline(t *testing.T, env *setup.ServerEnv) *model.Pipeline {
	t.Helper()
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create gated-secret pipeline")
	require.NotNil(t, created)
	require.Equal(t, model.StatusBlocked, created.Status, "a pipeline needing a gated secret must block at parse")
	stored, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusBlocked, stored.Status)
	assert.Equal(t, []string{gatedSecretName}, approvalErrorNames(t, stored))
	return stored
}

// TestGatedSecretApproverSessionReleases: a pipeline blocked at parse starts on an
// approver's browser-session approval, and its API JSON no longer names the secret.
func TestGatedSecretApproverSessionReleases(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)

	approved, err := pipeline.Approve(t.Context(), env.Store, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.NoError(t, err, "approve gated-secret pipeline")
	assert.Equal(t, env.Fixtures.Owner.Login, approved.Reviewer)

	finished := setup.WaitForPipeline(t, env.Store, approved.ID)
	assert.Equal(t, model.StatusSuccess, finished.Status)
	assert.Empty(t, approvalErrorNames(t, finished), "an approved pipeline has no approval error")
}

// TestGatedSecretTokenApprovalReblocks: an approval without a browser session
// (a JWT or API token) leaves the pipeline blocked.
func TestGatedSecretTokenApprovalReblocks(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)

	result, err := pipeline.Approve(t.Context(), env.Store, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: false})
	require.NoError(t, err)
	assert.Equal(t, model.StatusBlocked, result.Status)

	stored, err := env.Store.GetPipeline(blocked.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusBlocked, stored.Status)
	assert.Empty(t, stored.Reviewer, "a re-blocked pipeline records no reviewer")
	assert.Equal(t, []string{gatedSecretName}, approvalErrorNames(t, stored))
}

// TestGatedSecretEmptyApproversReleasesNothing: with no approvers configured even
// the owner's browser session cannot release the secret.
func TestGatedSecretEmptyApproversReleasesNothing(t *testing.T) {
	env := startGatedSecretEnv(t)
	server.Config.Pipeline.SecretApprovers = nil
	blocked := blockedGatedPipeline(t, env)

	result, err := pipeline.Approve(t.Context(), env.Store, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.NoError(t, err)
	assert.Equal(t, model.StatusBlocked, result.Status)
}

// TestGatedSecretDeclineNeverReleases: declining a blocked pipeline ends it without
// running any step.
func TestGatedSecretDeclineNeverReleases(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)

	declined, err := pipeline.Decline(t.Context(), env.Store, blocked, env.Fixtures.Owner, env.Fixtures.Repo)
	require.NoError(t, err)
	assert.Equal(t, model.StatusDeclined, declined.Status)
	steps, err := env.Store.StepList(declined.ID)
	require.NoError(t, err)
	for _, step := range steps {
		assert.NotEqual(t, model.StatusSuccess, step.State, "step %q ran after decline", step.Name)
	}
}

// TestGatedSecretRestartReblocks: a restart of an approved run resets Reviewer and
// Reviewed and blocks until a new approval.
func TestGatedSecretRestartReblocks(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)

	approved, err := pipeline.Approve(t.Context(), env.Store, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, approved.ID)
	require.Equal(t, model.StatusSuccess, finished.Status)
	require.NotEmpty(t, finished.Reviewer)

	restarted, err := pipeline.Restart(t.Context(), env.Store, finished, env.Fixtures.Owner, env.Fixtures.Repo, nil)
	require.NoError(t, err)
	assert.Equal(t, model.StatusBlocked, restarted.Status)

	stored, err := env.Store.GetPipeline(restarted.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusBlocked, stored.Status)
	assert.Empty(t, stored.Reviewer)
	assert.Zero(t, stored.Reviewed)
	assert.Equal(t, []string{gatedSecretName}, approvalErrorNames(t, stored))

	reapproved, err := pipeline.Approve(t.Context(), env.Store, stored, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.NoError(t, err)
	refinished := setup.WaitForPipeline(t, env.Store, reapproved.ID)
	assert.Equal(t, model.StatusSuccess, refinished.Status)
}

// TestGatedSecretTrunkMergeRefWithheld: hostile merge-queue config that requests a
// gated secret gets none, and fails instead of waiting for an approval that could
// never release it.
func TestGatedSecretTrunkMergeRefWithheld(t *testing.T) {
	env := startGatedSecretEnv(t)
	for name, p := range map[string]*model.Pipeline{
		"push": env.DummyPipeline(model.EventPush),
		"pull": env.DummyPipeline(model.EventPull),
	} {
		t.Run(name, func(t *testing.T) {
			if p.Event == model.EventPull {
				// GitHub names the merge-queue head only in the Refspec.
				p.Ref = "refs/pull/5/head"
				p.Refspec = "trunk-merge/main/5:main"
			} else {
				p.Branch = "trunk-merge/main/1"
				p.Ref = "refs/heads/trunk-merge/main/1"
			}

			created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, p)
			require.NoError(t, err)
			stored, err := env.Store.GetPipeline(created.ID)
			require.NoError(t, err)
			assert.Equal(t, model.StatusError, stored.Status, "a trunk-merge run must never wait for or get a gated secret")
			assert.Empty(t, approvalErrorNames(t, stored))
		})
	}
}

// TestGatedSecretApproveAfterDeclineRejected: once declined, a run can never be
// approved into the queue.
func TestGatedSecretApproveAfterDeclineRejected(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)
	// Both reviewers read the blocked row before either acts.
	staleCopy := *blocked

	_, err := pipeline.Decline(t.Context(), env.Store, blocked, env.Fixtures.Owner, env.Fixtures.Repo)
	require.NoError(t, err)

	_, err = pipeline.Approve(t.Context(), env.Store, &staleCopy, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.Error(t, err, "an approve racing a decline must be rejected")

	stored, err := env.Store.GetPipeline(blocked.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusDeclined, stored.Status)
	info := env.Queue.Info(t.Context())
	assert.Empty(t, info.Pending, "nothing may be queued after a decline")
	assert.Empty(t, info.Running)
	assert.Empty(t, info.WaitingOnDeps)
}

const branchSecretName = "PULUMI_ACCESS_TOKEN"

var branchSecretYAML = []byte(`
steps:
  - name: deploy
    image: dummy
    environment:
      TOKEN:
        from_secret: ` + branchSecretName + `
    commands:
      - echo deploy
`)

// TestBranchSecretWithheldFromCallerVariables: a manual run or restart with
// caller-supplied variables never gets a main-only secret, since a variable like
// BASH_ENV runs the caller's code in every step.
func TestBranchSecretWithheldFromCallerVariables(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: branchSecretYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	require.NoError(t, env.Store.SecretCreate(&model.Secret{
		RepoID:   env.Fixtures.Repo.ID,
		Name:     branchSecretName,
		Value:    "branch-filtered-value",
		Events:   []model.WebhookEvent{model.EventPush, model.EventManual},
		Branches: []string{"main"},
	}))

	manual := env.DummyPipeline(model.EventManual)
	manual.AdditionalVariables = map[string]string{"BASH_ENV": "/tmp/payload"}
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, manual)
	require.NoError(t, err)
	stored, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusError, stored.Status, "a manual run with variables must not get the secret")

	plain, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventManual))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, plain.ID)
	require.Equal(t, model.StatusSuccess, finished.Status, "a manual run without variables gets the secret")

	_, err = pipeline.Restart(t.Context(), env.Store, finished, env.Fixtures.Owner, env.Fixtures.Repo,
		map[string]string{"BASH_ENV": "/tmp/payload"})
	require.Error(t, err, "a restart with variables must not get the secret")
	restarted, err := env.Store.GetPipelineNumber(env.Fixtures.Repo, finished.Number+1)
	require.NoError(t, err)
	assert.Equal(t, model.StatusError, restarted.Status)
}

// TestBranchSecretWithheldFromCronVariables: a cron whose variables include
// BASH_ENV never gets a main-only secret; the same cron without variables does.
func TestBranchSecretWithheldFromCronVariables(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: branchSecretYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	require.NoError(t, env.Store.SecretCreate(&model.Secret{
		RepoID:   env.Fixtures.Repo.ID,
		Name:     branchSecretName,
		Value:    "branch-filtered-value",
		Events:   []model.WebhookEvent{model.EventCron},
		Branches: []string{"main"},
	}))
	env.Forge.On("BranchHead", mock.Anything, mock.Anything, mock.Anything, "main").
		Return(&model.Commit{SHA: "deadbeef"}, nil).Maybe()

	run := func(t *testing.T, name string, variables map[string]string) *model.Pipeline {
		t.Helper()
		c := &model.Cron{Name: name, RepoID: env.Fixtures.Repo.ID, Branch: "main", Schedule: "@daily", Variables: variables}
		repo, p, err := cron.CreatePipeline(t.Context(), env.Store, c)
		require.NoError(t, err)
		created, err := pipeline.Create(t.Context(), env.Store, repo, p)
		require.NoError(t, err)
		return created
	}

	withVariables := run(t, "with-variables", map[string]string{"BASH_ENV": "/tmp/payload"})
	stored, err := env.Store.GetPipeline(withVariables.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusError, stored.Status, "a cron with variables must not get the secret")

	control := run(t, "no-variables", nil)
	finished := setup.WaitForPipeline(t, env.Store, control.ID)
	assert.Equal(t, model.StatusSuccess, finished.Status, "a cron without variables gets the secret")
}

// startBranchSecretEnv starts a server whose workflow asks for a main-only secret
// that deployments may use, with the forge reporting mainHead as main's head.
func startBranchSecretEnv(t *testing.T, mainHead string) *setup.ServerEnv {
	t.Helper()
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: branchSecretYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	require.NoError(t, env.Store.SecretCreate(&model.Secret{
		RepoID:   env.Fixtures.Repo.ID,
		Name:     branchSecretName,
		Value:    "branch-filtered-value",
		Events:   []model.WebhookEvent{model.EventPush, model.EventDeploy},
		Branches: []string{"main"},
	}))
	env.Forge.On("BranchHead", mock.Anything, mock.Anything, mock.Anything, "main").
		Return(&model.Commit{SHA: mainHead}, nil).Maybe()
	return env
}

// TestBranchSecretDeploymentNeedsBranchHead: a deployment names its branch, but
// only a commit at an allowed branch's head proves the code came from it.
func TestBranchSecretDeploymentNeedsBranchHead(t *testing.T) {
	const mainHead = "a1b2c3d4"
	env := startBranchSecretEnv(t, mainHead)

	t.Run("a feature commit deployed as main gets no secret", func(t *testing.T) {
		deploy := env.DummyPipeline(model.EventDeploy)
		deploy.Commit = "feedface"
		deploy.Ref = "feedface"
		created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, deploy)
		require.NoError(t, err)
		stored, err := env.Store.GetPipeline(created.ID)
		require.NoError(t, err)
		assert.Equal(t, model.StatusError, stored.Status, "a deployment off main's head must not get a main-only secret")
	})

	t.Run("main's head deployed gets the secret", func(t *testing.T) {
		deploy := env.DummyPipeline(model.EventDeploy)
		deploy.Commit = mainHead
		created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, deploy)
		require.NoError(t, err)
		finished := setup.WaitForPipeline(t, env.Store, created.ID)
		assert.Equal(t, model.StatusSuccess, finished.Status)
	})
}

// TestBranchSecretPullRestartedAsDeploymentWithheld: restarting a pull request run
// as a deployment keeps Branch at the PR base, so it must not get a main-only
// secret, even when the forge would report the PR head as main's head.
func TestBranchSecretPullRestartedAsDeploymentWithheld(t *testing.T) {
	const prHead = "c0ffee00"
	env := startBranchSecretEnv(t, prHead)

	pull := env.DummyPipeline(model.EventPull)
	pull.Commit = prHead
	pull.Ref = "refs/pull/5/head"
	pull.Refspec = "feature:main"
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, pull)
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	// What the restart API does for ?event=deployment: it edits the copy in place.
	finished.Event = model.EventDeploy
	_, err = pipeline.Restart(t.Context(), env.Store, finished, env.Fixtures.Owner, env.Fixtures.Repo, nil)
	require.Error(t, err, "a PR run restarted as a deployment must not get a main-only secret")
	restarted, err := env.Store.GetPipelineNumber(env.Fixtures.Repo, finished.Number+1)
	require.NoError(t, err)
	assert.Equal(t, model.StatusError, restarted.Status)
	assert.True(t, restarted.EventOverridden, "the marker must persist for later re-parses")
}
