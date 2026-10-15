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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
	afterFinalize func(*model.Pipeline)
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
	if err := s.Store.UpdatePipelineIfStatus(p, from); err != nil {
		return err
	}
	// Approve's final write keeps the pipeline pending; the queue start follows it.
	if s.afterFinalize != nil && from == model.StatusPending && p.Status == model.StatusPending {
		hook := s.afterFinalize
		s.afterFinalize = nil
		hook(p)
	}
	return nil
}

// cancelOnLockWait starts pipeline.Cancel for p on another goroutine and returns
// once that Cancel is about to wait for the pipeline lock the caller holds. The
// returned channel yields Cancel's result. If Cancel finishes without waiting,
// the lock is missing and the hook returns early so the test fails, not hangs.
func cancelOnLockWait(t *testing.T, env *setup.ServerEnv, p *model.Pipeline) <-chan error {
	t.Helper()
	waiting := make(chan struct{})
	var once sync.Once
	pipeline.SetLockWaitHook(func(id int64) {
		if id == p.ID {
			once.Do(func() { close(waiting) })
		}
	})
	t.Cleanup(func() { pipeline.SetLockWaitHook(nil) })

	done := make(chan error, 1)
	go func() {
		done <- pipeline.Cancel(context.WithoutCancel(t.Context()), env.Forge, env.Store, env.Fixtures.Repo, env.Fixtures.Owner, p, &model.CancelInfo{})
	}()
	result := make(chan error, 1)
	select {
	case <-waiting:
		go func() { result <- <-done }()
	case err := <-done:
		result <- err
		t.Error("Cancel finished while Approve held the pipeline; it must wait for the lock")
	}
	return result
}

// assertCanceledRun asserts the pipeline was stopped and every workflow and step
// reached a terminal state without succeeding.
func assertCanceledRun(t *testing.T, env *setup.ServerEnv, id int64) {
	t.Helper()
	finished := setup.WaitForPipeline(t, env.Store, id)
	assert.Contains(t, []model.StatusValue{model.StatusKilled, model.StatusCanceled}, finished.Status)
	var workflows []*model.Workflow
	require.Eventually(t, func() bool {
		var err error
		workflows, err = env.Store.WorkflowGetTree(finished)
		require.NoError(t, err)
		for _, w := range workflows {
			if !stoppedState(w.State) {
				return false
			}
			for _, s := range w.Children {
				if !stoppedState(s.State) {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "every workflow and step must stop after cancel")
	require.NotEmpty(t, workflows, "the approval must have persisted workflows")
	for _, w := range workflows {
		assert.NotEqual(t, model.StatusSuccess, w.State, "workflow %q ran to success after cancel", w.Name)
	}
	require.Eventually(t, func() bool {
		info := env.Queue.Info(t.Context())
		return len(info.Pending) == 0 && len(info.Running) == 0 && len(info.WaitingOnDeps) == 0
	}, 30*time.Second, 50*time.Millisecond, "a canceled run must leave nothing queued or running")
}

func stoppedState(s model.StatusValue) bool {
	return s == model.StatusSkipped || s == model.StatusKilled || s == model.StatusCanceled || s == model.StatusFailure
}

func assertNothingQueued(t *testing.T, env *setup.ServerEnv) {
	t.Helper()
	info := env.Queue.Info(t.Context())
	assert.Empty(t, info.Pending, "a canceled approval must queue nothing")
	assert.Empty(t, info.Running)
	assert.Empty(t, info.WaitingOnDeps)
}

// slowGatedSecretYAML keeps the released step running long enough for a cancel
// that follows the approval to find it unfinished.
var slowGatedSecretYAML = []byte(`
steps:
  - name: deploy
    image: dummy
    environment:
      SLEEP: "30s"
      TOKEN:
        from_secret: ` + gatedSecretName + `
    commands:
      - echo deploy
`)

// TestCancelDuringApproveReplaceWaitsThenCancels: a cancel issued while Approve
// replaces workflows waits for the approval, then stops the run it started.
func TestCancelDuringApproveReplaceWaitsThenCancels(t *testing.T) {
	env := startGatedSecretEnvWith(t, slowGatedSecretYAML)
	blocked := blockedGatedPipeline(t, env)
	var cancelDone <-chan error
	hooked := &replaceHookStore{Store: env.Store, beforeReplace: func(*model.Pipeline) {
		cancelDone = cancelOnLockWait(t, env, blocked)
	}}

	approved, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.NoError(t, err, "the approval holds the pipeline, so the cancel cannot cut it short")
	require.NotNil(t, approved)
	require.NoError(t, <-cancelDone, "the waiting cancel must stop the approved run")
	assertCanceledRun(t, env, blocked.ID)
}

// TestCancelBetweenApproveFinalizeAndEnqueueWaitsThenCancels: a cancel issued after
// Approve's final write but before the queue start waits for the enqueue, then
// evicts the queued work.
func TestCancelBetweenApproveFinalizeAndEnqueueWaitsThenCancels(t *testing.T) {
	env := startGatedSecretEnvWith(t, slowGatedSecretYAML)
	blocked := blockedGatedPipeline(t, env)
	var cancelDone <-chan error
	hooked := &replaceHookStore{Store: env.Store, afterFinalize: func(*model.Pipeline) {
		cancelDone = cancelOnLockWait(t, env, blocked)
	}}

	_, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.NoError(t, err)
	require.NotNil(t, cancelDone, "the finalize hook must have run")
	require.NoError(t, <-cancelDone)
	assertCanceledRun(t, env, blocked.ID)
}

// TestCancelDuringFailedApprovePersistCancelsReblocked: a cancel waiting on an
// approval whose workflow persist fails cancels the re-blocked pipeline.
func TestCancelDuringFailedApprovePersistCancelsReblocked(t *testing.T) {
	env := startGatedSecretEnv(t)
	blocked := blockedGatedPipeline(t, env)
	var cancelDone <-chan error
	hooked := &replaceHookStore{
		Store:         env.Store,
		beforeReplace: func(*model.Pipeline) { cancelDone = cancelOnLockWait(t, env, blocked) },
		replaceErr:    errors.New("persist failed"),
	}

	_, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
		&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
	require.Error(t, err)
	require.NoError(t, <-cancelDone)

	stored, err := env.Store.GetPipeline(blocked.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCanceled, stored.Status)
	assertNothingQueued(t, env)
}

// TestApproveClaimSurvivesUnlockedStatusWrite: the claim compare-and-swaps stay as
// a backstop for a writer that bypasses the pipeline lock; its status stands and
// nothing is queued.
func TestApproveClaimSurvivesUnlockedStatusWrite(t *testing.T) {
	for name, replaceErr := range map[string]error{
		"finalize": nil,
		"rollback": errors.New("persist failed"),
	} {
		t.Run(name, func(t *testing.T) {
			env := startGatedSecretEnv(t)
			blocked := blockedGatedPipeline(t, env)
			hooked := &replaceHookStore{
				Store: env.Store,
				beforeReplace: func(p *model.Pipeline) {
					stored, err := env.Store.GetPipeline(p.ID)
					require.NoError(t, err)
					require.Equal(t, model.StatusPending, stored.Status, "the hook must run inside the claim window")
					stored.Status = model.StatusKilled
					require.NoError(t, env.Store.UpdatePipeline(stored))
				},
				replaceErr: replaceErr,
			}

			_, err := pipeline.Approve(t.Context(), hooked, blocked, env.Fixtures.Owner, env.Fixtures.Repo,
				&pipeline.Approval{Login: env.Fixtures.Owner.Login, Session: true})
			require.Error(t, err, "an approval overtaken by another writer must report it")

			stored, err := env.Store.GetPipeline(blocked.ID)
			require.NoError(t, err)
			assert.Equal(t, model.StatusKilled, stored.Status)
			assertNothingQueued(t, env)
		})
	}
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
