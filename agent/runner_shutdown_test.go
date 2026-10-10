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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types/mocks"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	rpc_mocks "go.woodpecker-ci.org/woodpecker/v3/rpc/mocks"
)

func TestRunReportsAgentShutdown(t *testing.T) {
	root, cancelRoot := context.WithCancelCause(t.Context())
	defer cancelRoot(nil)
	cancelRoot(pipeline_errors.ErrAgentShutdown)

	var done rpc.WorkflowState
	peer := shutdownPeer(t, &done, dummyWorkflow(), false, nil)
	runner := NewRunner(peer, rpc.Filter{}, "test-agent", &State{Metadata: map[string]Info{}}, shutdownBackend(t, nil))

	require.NoError(t, runner.Run(root))
	assert.True(t, done.Canceled)
	assert.True(t, done.AgentShutdown)
}

func TestRunPreservesEarlierCancellationCause(t *testing.T) {
	for _, tt := range []struct {
		name     string
		workflow *rpc.Workflow
		waitStop bool
	}{
		{name: "Wait cancellation", workflow: dummyWorkflow(), waitStop: true},
		{name: "workflow timeout", workflow: timeoutWorkflow()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root, cancelRoot := context.WithCancelCause(t.Context())
			defer cancelRoot(nil)
			stepCanceled := make(chan struct{})

			var done rpc.WorkflowState
			peer := shutdownPeer(t, &done, tt.workflow, tt.waitStop, nil)
			engine := shutdownBackend(t, func(ctx context.Context) {
				<-ctx.Done()
				close(stepCanceled)
				<-root.Done()
			})
			runner := NewRunner(peer, rpc.Filter{}, "test-agent", &State{Metadata: map[string]Info{}}, engine)
			finished := make(chan error, 1)
			go func() { finished <- runner.Run(root) }()

			select {
			case <-stepCanceled:
				cancelRoot(pipeline_errors.ErrAgentShutdown)
			case <-time.After(time.Second):
				t.Fatal("workflow was not canceled before root shutdown")
			}

			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("runner did not finish")
			}
			assert.True(t, done.Canceled)
			assert.False(t, done.AgentShutdown)
		})
	}
}

func TestRunShutdownDoneContext(t *testing.T) {
	root := context.WithValue(t.Context(), runnerContextKey{}, "runner-value")
	root = metadata.NewOutgoingContext(root, metadata.Pairs("runner-metadata", "value"))
	runnerCtx, cancelRoot := context.WithCancelCause(root)
	defer cancelRoot(nil)
	cancelRoot(pipeline_errors.ErrAgentShutdown)

	var done rpc.WorkflowState
	var doneCtx context.Context
	var doneContextLive bool
	peer := shutdownPeer(t, &done, dummyWorkflow(), false, func(ctx context.Context) {
		doneCtx = ctx
		doneContextLive = ctx.Err() == nil
	})

	runner := NewRunner(peer, rpc.Filter{}, "test-agent", &State{Metadata: map[string]Info{}}, shutdownBackend(t, nil))
	require.NoError(t, runner.Run(runnerCtx))
	assert.True(t, doneContextLive)
	require.NotNil(t, doneCtx)

	assert.Equal(t, "runner-value", doneCtx.Value(runnerContextKey{}))
	md, ok := metadata.FromOutgoingContext(doneCtx)
	require.True(t, ok)
	assert.Equal(t, []string{"value"}, md.Get("runner-metadata"))
}

type runnerContextKey struct{}

func timeoutWorkflow() *rpc.Workflow {
	workflow := dummyWorkflow()
	workflow.Timeout = -1
	return workflow
}

func shutdownBackend(t *testing.T, onCancel func(context.Context)) *mocks.MockBackend {
	t.Helper()

	engine := mocks.NewMockBackend(t)
	engine.On("SetupWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("DestroyWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("StartStep", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			ctx, ok := args.Get(0).(context.Context)
			if !ok {
				t.Error("StartStep called without a context")
				return
			}
			<-ctx.Done()
			if onCancel != nil {
				onCancel(ctx)
			}
		}).Return(errors.New("step interrupted"))
	return engine
}

func shutdownPeer(t *testing.T, done *rpc.WorkflowState, workflow *rpc.Workflow, waitCanceled bool, onDone func(context.Context)) *rpc_mocks.MockPeer {
	t.Helper()

	peer := rpc_mocks.NewMockPeer(t)
	peer.On("Next", mock.Anything, mock.Anything).Return(workflow, nil)
	peer.On("Init", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	peer.On("Wait", mock.Anything, mock.Anything).Return(waitCanceled, nil)
	peer.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	peer.On("Done", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			state, ok := args.Get(2).(rpc.WorkflowState)
			if !ok {
				t.Error("Done called without a WorkflowState")
				return
			}
			*done = state
			if onDone != nil {
				ctx, ok := args.Get(0).(context.Context)
				if !ok {
					t.Error("Done called without a context")
					return
				}
				onDone(ctx)
			}
		}).Return(nil)
	return peer
}
