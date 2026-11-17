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
<<<<<<< conflict 1 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	"context"
	"errors"
	"strconv"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"context"
+	"errors"
+	"fmt"
>>>>>>> conflict 1 of 3 ends
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
<<<<<<< conflict 2 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"github.com/stretchr/testify/require"
>>>>>>> conflict 2 of 3 ends

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
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
	store.EXPECT().TaskInsertBatch([]*model.Task{task}).Return(nil).Once()
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()
	store.EXPECT().TaskInsert(mock.MatchedBy(func(snapshot *model.Task) bool {
		return snapshot.ID == task.ID && snapshot.Enqueued == task.Enqueued && snapshot.Started == 0
	})).Run(func(*model.Task) {
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

func TestPersistentQueueRequeueAcceptsSurvivingBackupRow(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	store := store_mocks.NewMockStore(t)
	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	got, err := q.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NoError(t, q.Reserve(ctx, got.ID, 1, false))

	store.EXPECT().TaskInsert(mock.Anything).Return(types.ErrInsertDuplicateDetected).Once()
	store.EXPECT().TaskDelete(task.ID).Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Once()
	pq := &persistentQueue{Queue: q, store: store}
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

func TestPersistentQueuePausePersistsState(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "true").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}
	pq.Pause()

	assert.True(t, pq.Info(ctx).Paused)
}

func TestPersistentQueueResumePersistsState(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "true").Return(nil).Once()
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "false").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}
	pq.Pause()
	pq.Resume()

	assert.False(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreRestoresPausedState(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("true", nil).Once()

	pq := WithTaskStore(ctx, q, store)

	assert.True(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreLeavesQueueRunningWhenNotPaused(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("false", nil).Once()

	pq := WithTaskStore(ctx, q, store)

	assert.False(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreLeavesQueueRunningWhenNoPausedConfig(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("", types.ErrRecordNotExist).Once()

	pq := WithTaskStore(ctx, q, store)

	assert.False(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreRestoresPausedStateAfterRestart(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "true").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}
	pq.Pause()
	assert.True(t, pq.Info(ctx).Paused)

	// Fresh in-memory queue after process restart; paused flag comes from store.
	ctx2, cancel2, q2 := setupTestQueue(t)
	defer cancel2(nil)

	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("true", nil).Once()

	restored := WithTaskStore(ctx2, q2, store)
	assert.True(t, restored.Info(ctx2).Paused)
}
<<<<<<< conflict 3 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"

func TestPersistentQueueCancelConflictPreservesStore(t *testing.T) {
	for _, mode := range []string{"assignment before init", "reserved", "expired", "missing", "transaction failure"} {
		t.Run(mode, func(t *testing.T) {
			s := newQueueSQLiteStore(t)
			ctx, _, q := setupTestQueue(t)
			workflows := []*model.Workflow{
				{PipelineID: 1, PID: 1, Name: "pending", State: model.StatusPending},
				{PipelineID: 1, PID: 2, Name: "other", State: model.StatusPending},
			}
			require.NoError(t, s.WorkflowsCreate(workflows))
			ids := []string{strconv.FormatInt(workflows[0].ID, 10), strconv.FormatInt(workflows[1].ID, 10)}
			pq := WithTaskStore(ctx, q, s)
			require.NoError(t, pq.PushAtOnce(ctx, []*model.Task{{ID: ids[0]}, {ID: ids[1]}}))
			if mode == "assignment before init" || mode == "reserved" || mode == "expired" {
				task, err := pq.Poll(ctx, 7, func(task *model.Task) (bool, int) { return task.ID == ids[1], 1 })
				require.NoError(t, err)
				require.Equal(t, ids[1], task.ID)
				if mode == "reserved" {
					require.NoError(t, pq.Reserve(ctx, ids[1], 7, false))
				}
				if mode == "expired" {
					q.Lock()
					q.running[ids[1]].expired = true
					q.Unlock()
				}
			}
			if mode == "missing" {
				ids[1] = "999999"
			}
			beforeQueue := pq.Info(ctx)
			beforeTasks, err := s.TaskList()
			require.NoError(t, err)
			injected := errors.New("transaction failure")
			err = pq.CancelPending(ctx, ids, func() error {
				if mode == "transaction failure" {
					return injected
				}
				return s.WorkflowSupersedePending(1, []int64{workflows[0].ID, workflows[1].ID}, 2)
			})
			if mode == "transaction failure" {
				require.ErrorIs(t, err, injected)
			} else {
				require.ErrorIs(t, err, ErrPendingConflict)
			}
			assert.Equal(t, beforeQueue, pq.Info(ctx))
			afterTasks, err := s.TaskList()
			require.NoError(t, err)
			assert.Equal(t, beforeTasks, afterTasks)
			for _, wf := range workflows {
				got, err := s.WorkflowLoad(wf.ID)
				require.NoError(t, err)
				assert.Equal(t, model.StatusPending, got.State)
				assert.Nil(t, got.SupersededBy)
			}
		})
	}
}

func TestPersistentQueueCancelPendingSurvivesReload(t *testing.T) {
	s := newQueueSQLiteStore(t)
	ctx, _, q := setupTestQueue(t)
	workflows := []*model.Workflow{
		{PipelineID: 1, PID: 1, Name: "producer", State: model.StatusPending},
		{PipelineID: 1, PID: 2, Name: "consumer", State: model.StatusPending, DependsOn: []string{"producer"}},
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	ids := []string{strconv.FormatInt(workflows[0].ID, 10), strconv.FormatInt(workflows[1].ID, 10)}
	pq := WithTaskStore(ctx, q, s)
	require.NoError(t, pq.PushAtOnce(ctx, []*model.Task{{ID: ids[0]}, {ID: ids[1], Dependencies: []string{ids[0]}}}))
	require.NoError(t, pq.CancelPending(ctx, ids, func() error {
		return s.WorkflowSupersedePending(1, []int64{workflows[0].ID, workflows[1].ID}, 2)
	}))
	stored, err := s.TaskList()
	require.NoError(t, err)
	assert.Empty(t, stored)
	ctx2, _, q2 := setupTestQueue(t)
	reloaded := WithTaskStore(ctx2, q2, s)
	for _, candidate := range []Queue{pq, reloaded} {
		pollCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		task, err := candidate.Poll(pollCtx, 1, filterFnTrue)
		cancel()
		assert.Nil(t, task)
		assert.Error(t, err)
	}
}

func TestPersistentQueueEnqueueAndReloadTimes(t *testing.T) {
	s := newQueueSQLiteStore(t)
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	task := &model.Task{ID: "queued-at"}
	pq := WithTaskStore(ctx, q, s)
	require.NoError(t, pq.PushAtOnce(ctx, []*model.Task{task}))

	stored, err := s.TaskList()
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.NotZero(t, stored[0].Enqueued)
	enqueued := stored[0].Enqueued

	ctx2, cancel2, q2 := setupTestQueue(t)
	defer cancel2(nil)
	reloaded := WithTaskStore(ctx2, q2, s)
	reloadedTask := findQueuedTask(t, reloaded.Info(ctx2), task.ID)
	assert.Equal(t, enqueued, reloadedTask.Enqueued)
	assert.Zero(t, reloadedTask.Started)

	stored, err = s.TaskList()
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Equal(t, enqueued, stored[0].Enqueued)
}

func TestPersistentQueueLegacyTaskKeepsUnknownEnqueueTimeAfterReload(t *testing.T) {
	s := newQueueSQLiteStore(t)
	legacy := &model.Task{ID: "legacy-zero"}
	require.NoError(t, s.TaskInsert(legacy))

	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	reloaded := WithTaskStore(ctx, q, s)
	assert.Zero(t, findQueuedTask(t, reloaded.Info(ctx), legacy.ID).Enqueued)

	stored, err := s.TaskList()
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Zero(t, stored[0].Enqueued)

	ctx2, cancel2, q2 := setupTestQueue(t)
	defer cancel2(nil)
	reloadedAgain := WithTaskStore(ctx2, q2, s)
	assert.Zero(t, findQueuedTask(t, reloadedAgain.Info(ctx2), legacy.ID).Enqueued)
}

func TestPersistentQueueRequeueClearsStartedInInfoAndStore(t *testing.T) {
	s := newQueueSQLiteStore(t)
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	task := &model.Task{ID: "requeued-times", Enqueued: 123}
	pq := WithTaskStore(ctx, q, s)
	require.NoError(t, pq.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.NotZero(t, got.Started)

	require.NoError(t, q.Reserve(ctx, got.ID, 1, false))
	require.NoError(t, pq.Requeue(ctx, got.ID))
	assert.Zero(t, findQueuedTask(t, pq.Info(ctx), task.ID).Started)

	stored, err := s.TaskList()
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Zero(t, stored[0].Started)
	assert.Equal(t, int64(123), stored[0].Enqueued)
}

func TestPersistentQueueExpiryRequeueClearsStartedInInfoAndStore(t *testing.T) {
	s := newQueueSQLiteStore(t)
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	task := &model.Task{ID: "expired-times", Enqueued: 123}
	pq := WithTaskStore(ctx, q, s)
	require.NoError(t, pq.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.NotZero(t, got.Started)

	q.Lock()
	q.running[got.ID].deadline = time.Now().Add(-time.Second)
	q.resubmitExpiredPipelines()
	q.Unlock()

	event := receiveExpiredTask(t, q)
	require.NoError(t, pq.Reserve(ctx, event.ID, event.AgentID, true))
	require.NoError(t, pq.Requeue(ctx, event.ID))
	assert.Zero(t, findQueuedTask(t, pq.Info(ctx), task.ID).Started)

	stored, err := s.TaskList()
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Zero(t, stored[0].Started)
	assert.Equal(t, int64(123), stored[0].Enqueued)
}

func newQueueSQLiteStore(t *testing.T) store.Store {
	t.Helper()
	s, err := datastore.NewEngine(&store.Opts{
		Driver: "sqlite3",
		Config: ":memory:",
		XORM:   store.XORM{MaxOpenConns: 1, MaxIdleConns: 1},
	})
	require.NoError(t, err)
	require.NoError(t, s.Migrate(t.Context(), true))
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})
	return s
}

func findQueuedTask(t *testing.T, info InfoT, id string) *model.Task {
	t.Helper()
	for _, tasks := range [][]*model.Task{info.Pending, info.WaitingOnDeps} {
		for _, task := range tasks {
			if task.ID == id {
				return task
			}
		}
	}
	require.FailNowf(t, "task not queued", "task %s not in %s", id, info.String())
	return nil
}
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+
+func newSQLiteStore(t *testing.T) store.Store {
+	t.Helper()
+
+	s, err := datastore.NewEngine(&store.Opts{
+		Driver: "sqlite3",
+		Config: ":memory:",
+		// in-memory sqlite loses its schema when the pool drops the connection
+		XORM: store.XORM{MaxOpenConns: 1, MaxIdleConns: 1},
+	})
+	require.NoError(t, err)
+	require.NoError(t, s.Migrate(t.Context(), true))
+	t.Cleanup(func() { _ = s.Close() })
+	return s
+}
+
+// A dependency can finish while its dependent task is still queued. The
+// queue tracks that outcome only in memory, so a reload (server restart or
+// HA leader change) must restore it, otherwise Task.ShouldRun() sees an empty
+// DepStatus and vacuously lets the dependent run (#7154).
+func TestWithTaskStoreRestoresDepStatusAfterReload(t *testing.T) {
+	failed := func(ctx context.Context, q Queue, id string) error {
+		return q.Error(ctx, id, errors.New("workflow finished with error"))
+	}
+	done := func(state model.StatusValue) func(context.Context, Queue, string) error {
+		return func(ctx context.Context, q Queue, id string) error {
+			return q.Done(ctx, id, state)
+		}
+	}
+
+	tests := []struct {
+		name     string
+		depState model.StatusValue
+		finish   func(context.Context, Queue, string) error
+		runOn    []string
+		wantRun  bool
+	}{
+		{
+			name:     "failed dependency blocks success gate",
+			depState: model.StatusFailure,
+			finish:   failed,
+			runOn:    []string{string(model.StatusSuccess)},
+			wantRun:  false,
+		},
+		{
+			name:     "succeeded dependency lets success gate run",
+			depState: model.StatusSuccess,
+			finish:   done(model.StatusSuccess),
+			runOn:    []string{string(model.StatusSuccess)},
+			wantRun:  true,
+		},
+		{
+			name:     "succeeded dependency blocks failure-only gate",
+			depState: model.StatusSuccess,
+			finish:   done(model.StatusSuccess),
+			runOn:    []string{string(model.StatusFailure)},
+			wantRun:  false,
+		},
+		{
+			name:     "skipped dependency blocks success gate",
+			depState: model.StatusSkipped,
+			finish:   done(model.StatusSkipped),
+			runOn:    []string{string(model.StatusSuccess)},
+			wantRun:  false,
+		},
+		{
+			name:     "killed dependency blocks success gate",
+			depState: model.StatusKilled,
+			finish:   done(model.StatusKilled),
+			runOn:    []string{string(model.StatusSuccess)},
+			wantRun:  false,
+		},
+	}
+
+	for _, tt := range tests {
+		t.Run(tt.name, func(t *testing.T) {
+			s := newSQLiteStore(t)
+			dep, gate := createDepAndGate(t, s, tt.runOn)
+
+			// first server: the dependency runs and finishes, the gate stays queued
+			ctx1, cancel1 := context.WithCancelCause(t.Context())
+			q1 := WithTaskStore(ctx1, NewMemoryQueue(ctx1), s)
+			require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{dep, gate}))
+			polled, err := q1.Poll(ctx1, 1, filterFnTrue)
+			require.NoError(t, err)
+			require.Equal(t, dep.ID, polled.ID)
+			setWorkflowState(t, s, dep.ID, tt.depState)
+			require.NoError(t, tt.finish(ctx1, q1, dep.ID))
+			cancel1(nil)
+
+			// second server: reload the still queued gate from the store
+			ctx2, cancel2 := context.WithCancelCause(t.Context())
+			defer cancel2(nil)
+			q2 := WithTaskStore(ctx2, NewMemoryQueue(ctx2), s)
+
+			reloaded := queuedTask(t, q2.Info(ctx2), gate.ID)
+			assert.Equal(t, tt.depState, reloaded.DepStatus[dep.ID])
+			assert.Equal(t, tt.wantRun, reloaded.ShouldRun())
+		})
+	}
+}
+
+// A dependency that has not finished yet is left to the live queue, which
+// sets its status once the agent reports back.
+func TestWithTaskStoreKeepsUnfinishedDepStatusEmpty(t *testing.T) {
+	s := newSQLiteStore(t)
+	dep, gate := createDepAndGate(t, s, []string{string(model.StatusSuccess)})
+	require.NoError(t, s.TaskInsert(gate))
+	setWorkflowState(t, s, dep.ID, model.StatusRunning)
+
+	ctx, cancel := context.WithCancelCause(t.Context())
+	defer cancel(nil)
+	q := WithTaskStore(ctx, NewMemoryQueue(ctx), s)
+
+	reloaded := queuedTask(t, q.Info(ctx), gate.ID)
+	assert.NotContains(t, reloaded.DepStatus, dep.ID)
+}
+
+// A dependency that cannot be loaded must not stop the other dependencies of
+// the task from being restored.
+func TestRestoreDepStatusSkipsUnloadableDependency(t *testing.T) {
+	s := store_mocks.NewMockStore(t)
+	s.EXPECT().WorkflowLoad(int64(1)).Return(nil, errors.New("db down")).Once()
+	s.EXPECT().WorkflowLoad(int64(2)).Return(&model.Workflow{State: model.StatusFailure}, nil).Once()
+
+	task := &model.Task{ID: "3", Dependencies: []string{"invalid", "1", "2"}}
+	restoreDepStatus(s, task)
+
+	assert.Equal(t, map[string]model.StatusValue{"2": model.StatusFailure}, task.DepStatus)
+}
+
+// createDepAndGate stores a pending dependency workflow and a pending gate
+// workflow depending on it, and returns their queue tasks.
+func createDepAndGate(t *testing.T, s store.Store, gateRunOn []string) (dep, gate *model.Task) {
+	t.Helper()
+
+	workflows := []*model.Workflow{
+		{PipelineID: 1, PID: 1, Name: "dep", State: model.StatusPending},
+		{PipelineID: 1, PID: 2, Name: "gate", State: model.StatusPending},
+	}
+	require.NoError(t, s.WorkflowsCreate(workflows))
+
+	dep = &model.Task{
+		ID:        fmt.Sprint(workflows[0].ID),
+		RunOn:     []string{string(model.StatusSuccess)},
+		DepStatus: make(map[string]model.StatusValue),
+	}
+	gate = &model.Task{
+		ID:           fmt.Sprint(workflows[1].ID),
+		Dependencies: []string{dep.ID},
+		RunOn:        gateRunOn,
+		DepStatus:    make(map[string]model.StatusValue),
+	}
+	return dep, gate
+}
+
+func setWorkflowState(t *testing.T, s store.Store, id string, state model.StatusValue) {
+	t.Helper()
+
+	var workflowID int64
+	_, err := fmt.Sscan(id, &workflowID)
+	require.NoError(t, err)
+	workflow, err := s.WorkflowLoad(workflowID)
+	require.NoError(t, err)
+	workflow.State = state
+	require.NoError(t, s.WorkflowUpdate(workflow))
+}
+
+func queuedTask(t *testing.T, info InfoT, id string) *model.Task {
+	t.Helper()
+
+	for _, task := range append(info.Pending, info.WaitingOnDeps...) {
+		if task.ID == id {
+			return task
+		}
+	}
+	require.FailNowf(t, "task not queued", "task %s not in %s", id, info.String())
+	return nil
+}
>>>>>>> conflict 3 of 3 ends
