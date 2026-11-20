// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package runtime

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/dummy"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/logging"
)

const workflowDeadline = 200 * time.Millisecond

// waitUntilDone models a step that never exits on its own: WaitStep returns
// only when its context is done, as the backend contract requires.
func waitUntilDone(ctx context.Context) (*backend_types.State, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// stalledStreamBackend tails a log stream that stays open until released,
// even after the workflow context is done.
type stalledStreamBackend struct {
	backend_types.Backend
	release <-chan struct{}
}

func (b *stalledStreamBackend) TailStep(_ context.Context, _ *backend_types.Step, _ string) (io.ReadCloser, error) {
	return stalledStream{release: b.release}, nil
}

func (b *stalledStreamBackend) WaitStep(ctx context.Context, _ *backend_types.Step, _ string) (*backend_types.State, error) {
	return waitUntilDone(ctx)
}

type stalledStream struct {
	release <-chan struct{}
}

func (s stalledStream) Read(_ []byte) (int, error) {
	<-s.release
	return 0, io.EOF
}

func (s stalledStream) Close() error { return nil }

// honoringWaitBackend runs the step on the dummy backend, but WaitStep only
// returns when its context is done.
type honoringWaitBackend struct {
	backend_types.Backend
}

func (b *honoringWaitBackend) WaitStep(ctx context.Context, _ *backend_types.Step, _ string) (*backend_types.State, error) {
	return waitUntilDone(ctx)
}

// assertFinishesAtDeadline runs a workflow whose step never exits and asserts
// that Run returns near the workflow deadline, not when a blocked call is released.
func assertFinishesAtDeadline(t *testing.T, engine backend_types.Backend, logger logging.Logger) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), workflowDeadline)
	t.Cleanup(cancel)

	r := New(
		&backend_types.Config{
			Stages: []*backend_types.Stage{
				{Steps: []*backend_types.Step{cmdStep("hang")}},
			},
		},
		engine,
		WithTracer(newTestTracer(t)),
		WithLogger(logger),
		WithContext(ctx),
	)

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- r.Run(t.Context()) }()

	select {
	case err := <-done:
		require.ErrorIs(t, err, pipeline_errors.ErrCancel,
			"a workflow that hits its deadline must report the cancellation")
		assert.Less(t, time.Since(start), workflowDeadline+2*time.Second,
			"the workflow must finish near its deadline")
	case <-time.After(workflowDeadline + 5*time.Second):
		t.Fatal("workflow hung past its deadline: a blocked call was not bounded by the workflow context")
	}
}

// (i) The log stream stays open past the workflow deadline. The drain must not
// hold the workflow.
func TestWorkflowDeadlineBoundsStalledLogStream(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	engine := &stalledStreamBackend{Backend: dummy.New(), release: release}
	drain := func(_ *backend_types.Step, rc io.ReadCloser) error {
		_, err := io.Copy(io.Discard, rc)
		return errors.Join(err, rc.Close())
	}

	assertFinishesAtDeadline(t, engine, drain)
}

// (ii) The log sender blocks after one line, as an rpc EnqueueLog does when its
// buffer is full. The step must still end at the workflow deadline.
func TestWorkflowDeadlineBoundsBlockedLogSend(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	engine := &honoringWaitBackend{Backend: dummy.New()}
	blockedSend := func(_ *backend_types.Step, rc io.ReadCloser) error {
		if _, err := io.ReadFull(rc, make([]byte, 1)); err != nil {
			return err
		}
		<-release
		return rc.Close()
	}

	assertFinishesAtDeadline(t, engine, blockedSend)
}
