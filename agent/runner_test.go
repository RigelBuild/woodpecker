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

package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/state"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	rpc_mocks "go.woodpecker-ci.org/woodpecker/v3/rpc/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

func TestCreateLoggerClosesMaskedOutputWithoutTrailingNewline(t *testing.T) {
	var entries []*rpc.LogEntry
	peer := rpc_mocks.NewMockPeer(t)
	peer.EXPECT().EnqueueLog(mock.Anything).Run(func(entry *rpc.LogEntry) {
		entries = append(entries, entry)
	}).Once()
	runner := Runner{client: peer}
	workflow := &rpc.Workflow{Config: &backend_types.Config{
		Secrets: []*backend_types.Secret{{Value: "supersecret"}, {Value: "supersecret-longer"}},
	}}
	step := &backend_types.Step{Name: "build", UUID: "step-uuid"}

	err := runner.createLogger(zerolog.Nop(), workflow)(step, io.NopCloser(strings.NewReader("prefix supersecret")))
	assert.NoError(t, err)
	if assert.Len(t, entries, 1) {
		assert.Equal(t, "prefix ********", string(entries[0].Data))
	}
}

func TestTracerMasksStepOutputs(t *testing.T) {
	peer := rpc_mocks.NewMockPeer(t)
	peer.EXPECT().Update(mock.Anything, "1", mock.Anything).Run(func(_ context.Context, _ string, got rpc.StepState) {
		assert.Equal(t, "reported", got.OutputsState)
		assert.Equal(t, map[string]string{"result": "prefix ********"}, got.Outputs)
	}).Return(nil).Once()
	runner := Runner{client: peer}
	workflow := &rpc.Workflow{ID: "1", Config: &backend_types.Config{Secrets: []*backend_types.Secret{{Value: "supersecret"}}}}
	err := runner.createTracer(t.Context(), zerolog.Nop(), workflow)(&state.State{
		CurrStep:      &backend_types.Step{UUID: "step"},
		CurrStepState: backend_types.State{Exited: true, OutputsState: "reported", Outputs: map[string]string{"result": "prefix supersecret"}},
	})
	assert.NoError(t, err)
}

func TestTracerRejectsOversizedMaskedOutputs(t *testing.T) {
	var logs bytes.Buffer
	peer := rpc_mocks.NewMockPeer(t)
	peer.EXPECT().Update(mock.Anything, "1", mock.Anything).Run(func(_ context.Context, _ string, got rpc.StepState) {
		assert.Equal(t, "invalid", got.OutputsState)
		assert.Nil(t, got.Outputs)
	}).Return(nil).Once()
	runner := Runner{client: peer}
	workflow := &rpc.Workflow{ID: "1", Config: &backend_types.Config{Secrets: []*backend_types.Secret{{Value: "abcd"}}}}
	value := strings.Repeat("abcdx", 600)
	err := runner.createTracer(t.Context(), zerolog.New(&logs), workflow)(&state.State{
		CurrStep:      &backend_types.Step{UUID: "step"},
		CurrStepState: backend_types.State{Exited: true, OutputsState: "reported", Outputs: map[string]string{"result": value}},
	})
	assert.NoError(t, err)
	assert.Contains(t, logs.String(), "masked step outputs exceed size limit")
	assert.Contains(t, logs.String(), `"level":"warn"`)
	assert.NotContains(t, logs.String(), value)
}

func dummyWorkflow() *rpc.Workflow {
	return &rpc.Workflow{
		ID: "1",
		Config: &backend_types.Config{
			Stages: []*backend_types.Stage{{
				Steps: []*backend_types.Step{{
					Name:      "build",
					UUID:      "step-uuid",
					OnSuccess: true,
					Environment: map[string]string{
						"CI_REPO":            "octocat/hello-world",
						"CI_PIPELINE_NUMBER": "1",
					},
				}},
			}},
		},
	}
}

// A workflow the server canceled did not fail, it was stopped on purpose. The
// cancellation is reported through WorkflowState.Canceled, so no error text
// must be reported next to it: the server stores that text as the workflow
// error and the web UI shows it as a runtime error.
func TestRunReportsCancelWithoutError(t *testing.T) {
	engine := mocks.NewMockBackend(t)
	engine.On("SetupWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("DestroyWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// The step start is aborted mid-flight once the workflow is canceled, the
	// backend surfacing whatever its transport made of the cancel cause.
	engine.On("StartStep", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			ctx, ok := args.Get(0).(context.Context)
			if !ok {
				t.Error("StartStep called without a context")
				return
			}
			<-ctx.Done()
		}).
		Return(errors.New(`error during connect: Post "http://.../containers/x/start": Canceled`))

	var done rpc.WorkflowState
	peer := rpc_mocks.NewMockPeer(t)
	peer.On("Next", mock.Anything, mock.Anything).Return(dummyWorkflow(), nil)
	peer.On("Init", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	peer.On("Wait", mock.Anything, mock.Anything).Return(true, nil)
	peer.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	peer.On("Done", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			state, ok := args.Get(2).(rpc.WorkflowState)
			if !ok {
				t.Error("Done called without a workflow state")
				return
			}
			done = state
		}).
		Return(nil)

	counter := &State{Metadata: map[string]Info{}}
	runner := NewRunner(peer, rpc.Filter{}, "test-agent", 1, counter, engine)

	assert.NoError(t, runner.Run(t.Context()))

	assert.True(t, done.Canceled, "the workflow must be reported as canceled")
	assert.Empty(t, done.Error, "a cancellation is not a workflow error")
}

func TestRunCancelsWhenLeaseIsNotFound(t *testing.T) {
	notFoundErr := status.Error(codes.NotFound, "queue: task not found")
	_, err := backoff.Retry(t.Context(), func() (struct{}, error) {
		return struct{}{}, backoff.Permanent(notFoundErr)
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.NotEqual(t, notFoundErr.Error(), status.Convert(err).Message())

	originalTimeout := constant.TaskTimeout
	constant.TaskTimeout = 30 * time.Millisecond
	t.Cleanup(func() { constant.TaskTimeout = originalTimeout })

	tests := []struct {
		name       string
		err        error
		wantCancel bool
	}{
		{
			name:       "server no longer knows workflow",
			err:        err,
			wantCancel: true,
		},
		{
			name: "transient error",
			err:  status.Error(codes.Unavailable, "server unavailable"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stepCtxCh := make(chan context.Context, 1)
			engine := mocks.NewMockBackend(t)
			engine.On("SetupWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			engine.On("DestroyWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			engine.On("StartStep", mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					ctx := args.Get(0).(context.Context)
					stepCtxCh <- ctx
					<-ctx.Done()
				}).Return(errors.New("step interrupted"))

			extendCalls := make(chan struct{}, 4)
			peer := rpc_mocks.NewMockPeer(t)
			peer.On("Next", mock.Anything, mock.Anything).Return(dummyWorkflow(), nil)
			peer.On("Init", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			peer.On("Wait", mock.Anything, mock.Anything).Return(false, nil)
			peer.On("Extend", mock.Anything, "1").Run(func(mock.Arguments) {
				select {
				case extendCalls <- struct{}{}:
				default:
				}
			}).Return(tt.err)
			peer.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			peer.On("Done", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			runner := NewRunner(peer, rpc.Filter{}, "test-agent", 1, &State{Metadata: map[string]Info{}}, engine)
			runnerCtx, cancelRunner := context.WithCancelCause(t.Context())
			runDone := make(chan error, 1)
			go func() { runDone <- runner.Run(runnerCtx) }()

			finished := false
			defer func() {
				if !finished {
					cancelRunner(nil)
					select {
					case <-runDone:
					case <-time.After(time.Second):
						t.Error("runner did not stop after test cleanup")
					}
				}
			}()

			var stepCtx context.Context
			select {
			case stepCtx = <-stepCtxCh:
			case <-time.After(time.Second):
				t.Fatal("workflow step did not start")
			}
			waitForExtend := func() {
				t.Helper()
				select {
				case <-extendCalls:
				case <-time.After(time.Second):
					t.Fatal("workflow lease was not extended")
				}
			}
			waitForExtend()

			if tt.wantCancel {
				select {
				case <-stepCtx.Done():
				case <-time.After(time.Second):
					t.Fatal("workflow context was not canceled after lease not found")
				}
			} else {
				select {
				case <-stepCtx.Done():
					t.Fatal("transient lease error canceled the workflow")
				case <-time.After(50 * time.Millisecond):
				}
				waitForExtend()
				cancelRunner(nil)
			}

			select {
			case err := <-runDone:
				require.NoError(t, err)
				finished = true
			case <-time.After(time.Second):
				t.Fatal("runner did not return")
			}
		})
	}
}

func TestFormatAgentLabels(t *testing.T) {
	// Empty label set renders as an empty string, not "map[]" or similar.
	assert.Empty(t, formatAgentLabels(nil))
	assert.Empty(t, formatAgentLabels(map[string]string{}))

	// Labels render as a JSON object with keys sorted, so the value is stable
	// across runs regardless of map iteration order.
	got := formatAgentLabels(map[string]string{
		"platform": "linux/amd64",
		"backend":  "docker",
		"hostname": "agent-1",
		"repo":     "*",
	})
	assert.JSONEq(t, `{"backend":"docker","hostname":"agent-1","platform":"linux/amd64","repo":"*"}`, got)

	assert.Equal(t, `{"gpu":"true"}`, formatAgentLabels(map[string]string{"gpu": "true"}))
}
