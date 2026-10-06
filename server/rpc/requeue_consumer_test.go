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

package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestConsumeExpiredKeepsGoingAfterError(t *testing.T) {
	events := make(chan queue.ExpiredTask, 2)
	sched := scheduler_mocks.NewMockScheduler(t)
	sched.On("Expired").Return((<-chan queue.ExpiredTask)(events))
	mockStore := store_mocks.NewMockStore(t)
	loaded := make(chan int64, 2)
	mockStore.On("WorkflowLoad", int64(30)).Run(func(args mock.Arguments) { loaded <- 30 }).Return(nil, errors.New("db down")).Once()
	mockStore.On("WorkflowLoad", int64(31)).Run(func(args mock.Arguments) { loaded <- 31 }).Return(nil, errors.New("db down")).Once()

	r := &RPC{store: mockStore, scheduler: sched}
	ctx, cancel := context.WithCancelCause(t.Context())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		r.consumeExpired(ctx)
	}()

	events <- queue.ExpiredTask{ID: "30", AgentID: 1}
	events <- queue.ExpiredTask{ID: "31", AgentID: 1}
	for _, want := range []int64{30, 31} {
		select {
		case got := <-loaded:
			require.Equal(t, want, got)
		case <-time.After(10 * time.Second):
			t.Fatalf("event for workflow %d was not handled", want)
		}
	}

	cancel(nil)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop with its context")
	}
}

func TestHandleExpiredUsesCallerContext(t *testing.T) {
	type key struct{}
	sched := scheduler_mocks.NewMockScheduler(t)
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: 20, PID: 1, State: model.StatusRunning, AgentID: 1}, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.On("GetPipeline", int64(20)).Return(&model.Pipeline{ID: 20, RepoID: 10}, nil)
	mockStore.On("GetRepo", int64(10)).Return(&model.Repo{ID: 10}, nil)
	var reserveCtx context.Context
	sched.On("Reserve", mock.Anything, "30", int64(1), true).Run(func(args mock.Arguments) {
		ctx, ok := args.Get(0).(context.Context)
		if !ok {
			t.Error("Reserve called without a context")
			return
		}
		reserveCtx = ctx
	}).Return(queue.ErrNotFound)

	ctx := context.WithValue(t.Context(), key{}, "server")
	r := &RPC{store: mockStore, scheduler: sched}
	require.NoError(t, r.HandleExpired(ctx, queue.ExpiredTask{ID: "30", AgentID: 1}))
	require.NotNil(t, reserveCtx)
	require.Equal(t, "server", reserveCtx.Value(key{}))
}
