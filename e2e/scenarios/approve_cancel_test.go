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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// replaceHookStore runs a hook in place of, or just before, the workflow
// replacement Approve performs after it claims a blocked pipeline. That is the
// window in which a concurrent Cancel can land.
type replaceHookStore struct {
	store.Store
	beforeReplace func(*model.Pipeline)
	replaceErr    error
	updateErr     error
}

func (s *replaceHookStore) WorkflowsReplace(p *model.Pipeline, workflows []*model.Workflow) error {
	if s.beforeReplace != nil {
		hook := s.beforeReplace
		s.beforeReplace = nil
		hook(p)
	}
	if s.replaceErr != nil {
		return s.replaceErr
	}
	return s.Store.WorkflowsReplace(p, workflows)
}

func (s *replaceHookStore) UpdatePipelineIfStatus(p *model.Pipeline, from model.StatusValue) error {
	if s.updateErr != nil && from == model.StatusPending {
		return s.updateErr
	}
	return s.Store.UpdatePipelineIfStatus(p, from)
}

// cancelDuringReplace cancels the pipeline through the real store while Approve
// sits between its claim and its workflow replacement.
func cancelDuringReplace(t *testing.T, env *setup.ServerEnv) func(*model.Pipeline) {
	t.Helper()
	return func(p *model.Pipeline) {
		stored, err := env.Store.GetPipeline(p.ID)
		require.NoError(t, err)
		require.Equal(t, model.StatusPending, stored.Status, "the hook must run inside the claim window")
		require.NoError(t, pipeline.Cancel(t.Context(), env.Forge, env.Store, env.Fixtures.Repo, env.Fixtures.Owner, stored, &model.CancelInfo{}))
	}
}

func assertNothingQueued(t *testing.T, env *setup.ServerEnv) {
	t.Helper()
	info := env.Queue.Info(t.Context())
	assert.Empty(t, info.Pending, "a canceled approval must queue nothing")
	assert.Empty(t, info.Running)
	assert.Empty(t, info.WaitingOnDeps)
}

// TestApproveLosesToConcurrentCancel: a cancel that lands after Approve claims the
// pipeline wins; the approval enqueues nothing and the canceled status stays.
func TestApproveLosesToConcurrentCancel(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)
	hooked := &replaceHookStore{Store: env.Store, beforeReplace: cancelDuringReplace(t, env)}

	_, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.Error(t, err, "an approval overtaken by a cancel must report it")

	stored, err := env.Store.GetPipeline(blocked.ID)
	require.NoError(t, err)
	assert.Contains(t, []model.StatusValue{model.StatusKilled, model.StatusCanceled}, stored.Status)
	assertNothingQueued(t, env)
	steps, err := env.Store.StepList(blocked.ID)
	require.NoError(t, err)
	for _, step := range steps {
		assert.NotEqual(t, model.StatusSuccess, step.State, "step %q ran after cancel", step.Name)
		assert.NotEqual(t, model.StatusRunning, step.State, "step %q ran after cancel", step.Name)
	}
}

// TestApproveRollbackLosesToConcurrentCancel: when workflow persistence fails after
// a cancel, the rollback must not re-block the canceled pipeline.
func TestApproveRollbackLosesToConcurrentCancel(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)
	hooked := &replaceHookStore{
		Store:         env.Store,
		beforeReplace: cancelDuringReplace(t, env),
		replaceErr:    errors.New("persist failed"),
	}

	_, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.Error(t, err)

	stored, err := env.Store.GetPipeline(blocked.ID)
	require.NoError(t, err)
	assert.Contains(t, []model.StatusValue{model.StatusKilled, model.StatusCanceled}, stored.Status)
	assertNothingQueued(t, env)
}

// TestApproveRollbackFailureReturnsError: if both the workflow persist and the
// re-block fail, Approve must say so rather than leave a silent pending row.
func TestApproveRollbackFailureReturnsError(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)
	rollbackErr := errors.New("rollback failed")
	hooked := &replaceHookStore{
		Store:      env.Store,
		replaceErr: errors.New("persist failed"),
		updateErr:  rollbackErr,
	}

	_, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.ErrorIs(t, err, rollbackErr)
	assertNothingQueued(t, env)
}
