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

package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// A task that lingers in the in-memory queue but is already gone from the
// backup store must be dropped on Poll instead of being handed to the agent,
// otherwise it loops forever (re-poll, illegal-instruction, resubmit).
func TestPersistentQueuePollDropsStaleTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(types.ErrRecordNotExist).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Nil(t, got, "stale task must not be returned to the agent")

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending, "stale task must be removed from pending")
	assert.Equal(t, 0, info.Stats.Running, "stale task must be removed from running")
}

func TestPersistentQueuePollDropsTaskWithMissingWorkflow(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(nil, types.ErrRecordNotExist).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Nil(t, got, "task without a workflow must not be returned to the agent")

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending)
	assert.Equal(t, 0, info.Stats.Running)
}

func TestPersistentQueuePollDropsTerminalWorkflowTask(t *testing.T) {
	terminalStates := []model.StatusValue{
		model.StatusSuccess,
		model.StatusFailure,
		model.StatusKilled,
		model.StatusCanceled,
		model.StatusSkipped,
		model.StatusError,
		model.StatusDeclined,
	}

	for _, state := range terminalStates {
		t.Run(string(state), func(t *testing.T) {
			ctx, cancel, q := setupTestQueue(t)
			defer cancel(nil)

			store := store_mocks.NewMockStore(t)
			store.EXPECT().TaskDelete("1").Return(nil).Once()
			store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: state}, nil).Once()

			pq := &persistentQueue{Queue: q, store: store}

			task := genDummyTask()
			assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

			got, err := pq.Poll(ctx, 1, filterFnTrue)
			assert.NoError(t, err)
			assert.Nil(t, got, "terminal workflow task must not be returned to the agent")

			info := q.Info(ctx)
			assert.Equal(t, 0, info.Stats.Pending)
			assert.Equal(t, 0, info.Stats.Running)
		})
	}
}

// A task that is still present in the backup store and whose workflow is still
// active is polled normally.
func TestPersistentQueuePollReturnsLiveTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, "1", got.ID)
}

func TestPersistentQueueRequeuePersistsBeforePoll(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	store := store_mocks.NewMockStore(t)
	task := genDummyTask()
	store.EXPECT().TaskInsert(task).Return(nil).Once()
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()
	store.EXPECT().TaskInsert(task).Run(func(*model.Task) {
		assert.Equal(t, 1, q.Info(ctx).Stats.Running, "requeued task must remain reserved until its backup row is inserted")
	}).Return(nil).Once()
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()
	pq := &persistentQueue{Queue: q, store: store}

	assert.NoError(t, pq.PushAtOnce(ctx, []*model.Task{task}))
	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.NoError(t, q.Reserve(ctx, got.ID, 1, false))
	assert.NoError(t, pq.Requeue(ctx, got.ID))

	got, err = pq.Poll(ctx, 2, filterFnTrue)
	assert.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
}

func TestPersistentQueueRequeuePersistsTaskSnapshot(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	store := store_mocks.NewMockStore(t)
	task := &model.Task{
		ID:           "snapshot-task",
		Data:         []byte("payload"),
		Labels:       map[string]string{"team": "queue"},
		Dependencies: []string{"upstream"},
		RunOn:        []string{"success"},
		DepStatus:    map[string]model.StatusValue{"upstream": model.StatusPending},
	}
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	got, err := q.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NoError(t, q.Reserve(ctx, got.ID, 1, false))

	insertStarted := make(chan struct{})
	insertContinue := make(chan struct{})
	var persistedTask *model.Task
	store.EXPECT().TaskInsert(mock.Anything).Run(func(snapshot *model.Task) {
		persistedTask = snapshot
		close(insertStarted)
		<-insertContinue
	}).Return(nil).Once()
	pq := &persistentQueue{Queue: q, store: store}

	requeueResult := make(chan error, 1)
	go func() { requeueResult <- pq.Requeue(ctx, task.ID) }()
	select {
	case <-insertStarted:
	case <-time.After(time.Second):
		t.Fatal("requeue did not start persisting the task snapshot")
	}
	q.Lock()
	q.running[got.ID].item.Data[0] = 'P'
	q.running[got.ID].item.Labels["team"] = "changed"
	q.running[got.ID].item.Dependencies[0] = "changed-upstream"
	q.running[got.ID].item.RunOn[0] = "failure"
	q.running[got.ID].item.DepStatus["upstream"] = model.StatusSuccess
	q.Unlock()
	close(insertContinue)
	select {
	case err := <-requeueResult:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("requeue did not complete")
	}
	assert.NotSame(t, task, persistedTask)
	assert.Equal(t, model.StatusPending, persistedTask.DepStatus["upstream"])
	assert.Equal(t, []byte("payload"), persistedTask.Data)
	assert.Equal(t, map[string]string{"team": "queue"}, persistedTask.Labels)
	assert.Equal(t, []string{"upstream"}, persistedTask.Dependencies)
	assert.Equal(t, []string{"success"}, persistedTask.RunOn)
	assert.Equal(t, map[string]model.StatusValue{"upstream": model.StatusPending}, persistedTask.DepStatus)
	assert.Equal(t, model.StatusSuccess, task.DepStatus["upstream"])
	assert.Same(t, task, q.pending.Front().Value)
}

func TestPersistentQueueRequeueInsertFailureCanRetry(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	store := store_mocks.NewMockStore(t)
	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	got, err := q.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	q.Lock()
	q.running[got.ID].deadline = time.Now().Add(-time.Second)
	q.resubmitExpiredPipelines()
	q.Unlock()
	assert.Equal(t, ExpiredTask{ID: task.ID, AgentID: 1}, receiveExpiredTask(t, q))
	assert.NoError(t, q.Reserve(ctx, got.ID, 1, true))

	store.EXPECT().TaskInsert(mock.Anything).Return(errors.New("temporary insert failure")).Once()
	store.EXPECT().TaskInsert(mock.Anything).Return(nil).Once()
	store.EXPECT().TaskDelete(task.ID).Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()
	pq := &persistentQueue{Queue: q, store: store}
	assert.Error(t, pq.Requeue(ctx, task.ID))
	q.Lock()
	q.resubmitExpiredPipelines()
	q.Unlock()
	assert.Equal(t, ExpiredTask{ID: task.ID, AgentID: 1}, receiveExpiredTask(t, q))
	assert.NoError(t, q.Reserve(ctx, task.ID, 1, true))
	assert.NoError(t, pq.Requeue(ctx, task.ID))
	got, err = pq.Poll(ctx, 2, filterFnTrue)
	assert.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
}

func TestPersistentQueueRequeueDeletesBackupOnQueueFailure(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	store := store_mocks.NewMockStore(t)
	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	got, err := q.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NoError(t, q.Reserve(ctx, got.ID, 1, false))

	insertStarted := make(chan struct{})
	insertContinue := make(chan struct{})
	store.EXPECT().TaskInsert(mock.Anything).Run(func(*model.Task) {
		close(insertStarted)
		<-insertContinue
	}).Return(nil).Once()
	store.EXPECT().TaskDelete(task.ID).Return(nil).Once()
	pq := &persistentQueue{Queue: q, store: store}
	requeueResult := make(chan error, 1)
	go func() { requeueResult <- pq.Requeue(ctx, task.ID) }()
	select {
	case <-insertStarted:
	case <-time.After(time.Second):
		t.Fatal("requeue did not persist backup row")
	}
	assert.NoError(t, q.Done(ctx, task.ID, model.StatusSuccess))
	close(insertContinue)
	select {
	case err := <-requeueResult:
		assert.ErrorIs(t, err, ErrNotFound)
	case <-time.After(time.Second):
		t.Fatal("requeue did not observe that the task was finished")
	}
	assert.Equal(t, 0, q.Info(ctx).Stats.Running)
	assert.Equal(t, 0, q.Info(ctx).Stats.Pending)
}

func TestPersistentQueueDoneRemovesPendingTaskFromBackup(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	assert.NoError(t, pq.Done(ctx, task.ID, model.StatusSuccess))

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending)
	assert.Equal(t, 0, info.Stats.Running)
}

func TestPersistentQueueDoneIgnoresAlreadyRemovedBackupTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()
	store.EXPECT().TaskDelete("1").Return(types.ErrRecordNotExist).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.NoError(t, pq.Done(ctx, task.ID, model.StatusSuccess))

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending)
	assert.Equal(t, 0, info.Stats.Running)
}
