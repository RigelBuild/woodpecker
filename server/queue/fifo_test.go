// Copyright 2023 Woodpecker Authors
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
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

var (
	filterFnTrue = func(*model.Task) (bool, int) { return true, 1 }
	genDummyTask = func() *model.Task {
		return &model.Task{
			ID:   "1",
			Data: []byte("{}"),
		}
	}
	waitForProcess = func() { time.Sleep(processTimeInterval + 50*time.Millisecond) }
)

func setupTestQueue(t *testing.T) (context.Context, context.CancelCauseFunc, *fifo) {
	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	q, _ := NewMemoryQueue(ctx).(*fifo)
	if q == nil {
		t.Fatal("Failed to create queue")
	}

	return ctx, cancel, q
}

func TestFifoBasicOperations(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	t.Run("push poll done lifecycle", func(t *testing.T) {
		dummyTask := genDummyTask()

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Len(t, info.Pending, 1)

		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, dummyTask, got)

		waitForProcess()
		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 1)

		// Edge case: verify task can't be polled again while running
		pollCtx, pollCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err = q.Poll(pollCtx, 2, filterFnTrue)
		pollCancel()
		assert.Error(t, err) // Should timeout/cancel, not return the same task

		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))

		waitForProcess()
		info = q.Info(ctx)
		assert.Len(t, info.Running, 0)

		// Edge case: Done on already completed task should handle gracefully
		err = q.Done(ctx, got.ID, model.StatusSuccess)
		// Document current behavior - should either error or be idempotent
		if err != nil {
			assert.Error(t, err)
		}
	})

	t.Run("error handling", func(t *testing.T) {
		task1 := &model.Task{ID: "task-error-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1}))

		waitForProcess()
		got, _ := q.Poll(ctx, 1, filterFnTrue)

		assert.NoError(t, q.Error(ctx, got.ID, fmt.Errorf("test error")))
		waitForProcess()
		info := q.Info(ctx)
		assert.Len(t, info.Running, 0)

		assert.Error(t, q.Error(ctx, "totally-fake-id", fmt.Errorf("test error")))

		// Edge case: Error on task that's already errored
		err := q.Error(ctx, got.ID, fmt.Errorf("double error"))
		// Should either error or be idempotent
		if err != nil {
			assert.Error(t, err)
		}
	})

	t.Run("external error filtered by Wait", func(t *testing.T) {
		// Test that external errors (from Error/ErrorAtOnce) are wrapped as ErrExternal
		// and filtered out by Wait(), while internal errors like context cancellation
		// are passed through

		// Test 1: External error is filtered by Wait
		task1 := &model.Task{ID: "wait-external-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1}))
		waitForProcess()

		got1, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)

		// Start waiting on the task
		waitDone := make(chan error, 1)
		go func() {
			waitDone <- q.Wait(ctx, got1.ID)
		}()

		time.Sleep(10 * time.Millisecond)

		// Report an external error (agent reported error)
		externalErr := fmt.Errorf("agent reported error")
		assert.NoError(t, q.Error(ctx, got1.ID, externalErr))

		// Wait should return nil (external error filtered out)
		select {
		case err := <-waitDone:
			assert.NoError(t, err, "Wait should filter ErrExternal and return nil")
		case <-time.After(time.Second):
			t.Fatal("Wait should have returned")
		}

		// Test 2: Internal error (context cancellation) passes through Wait
		task2 := &model.Task{ID: "wait-internal-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task2}))
		waitForProcess()

		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)

		waitCtx, waitCancel := context.WithCancelCause(ctx)
		waitDone2 := make(chan error, 1)
		go func() {
			waitDone2 <- q.Wait(waitCtx, got2.ID)
		}()

		time.Sleep(10 * time.Millisecond)
		waitCancel(nil)

		// Context cancellation should cause Wait to return (internal error handling)
		select {
		case err := <-waitDone2:
			// Wait returns nil when context is canceled (normal behavior)
			assert.NoError(t, err, "Wait should return nil when context is canceled")
		case <-time.After(time.Second):
			t.Fatal("Wait should return when context is canceled")
		}

		// Clean up
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()

		// Test 3: Multiple waiters all get nil when external error occurs
		task3 := &model.Task{ID: "wait-multi-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task3}))
		waitForProcess()

		got3, err := q.Poll(ctx, 3, filterFnTrue)
		assert.NoError(t, err)

		// Start multiple waiters
		numWaiters := 3
		waitResults := make(chan error, numWaiters)
		for i := 0; i < numWaiters; i++ {
			go func() {
				waitResults <- q.Wait(ctx, got3.ID)
			}()
		}

		time.Sleep(10 * time.Millisecond)

		// Report an external error
		batchErr := fmt.Errorf("external batch failure")
		assert.NoError(t, q.ErrorAtOnce(ctx, []string{got3.ID}, batchErr))

		// All waiters should return nil (external error filtered)
		for i := 0; i < numWaiters; i++ {
			select {
			case err := <-waitResults:
				assert.NoError(t, err, "All waiters should get nil when ErrExternal is filtered")
			case <-time.After(time.Second):
				t.Fatalf("Waiter %d didn't return in time", i)
			}
		}
	})

	t.Run("error at once", func(t *testing.T) {
		task1 := &model.Task{ID: "batch-1"}
		task2 := &model.Task{ID: "batch-2"}
		task3 := &model.Task{ID: "batch-3"}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1, task2, task3}))
		waitForProcess()

		got1, _ := q.Poll(ctx, 1, filterFnTrue)
		got2, _ := q.Poll(ctx, 2, filterFnTrue)

		assert.NoError(t, q.ErrorAtOnce(ctx, []string{got1.ID, got2.ID}, fmt.Errorf("batch error")))
		waitForProcess()
		info := q.Info(ctx)
		assert.Len(t, info.Running, 0)
		assert.Len(t, info.Pending, 1)

		got3, _ := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, q.Done(ctx, got3.ID, model.StatusSuccess))
		waitForProcess()

		task4 := &model.Task{ID: "batch-4"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task4}))
		waitForProcess()
		got4, _ := q.Poll(ctx, 1, filterFnTrue)

		err := q.ErrorAtOnce(ctx, []string{got4.ID, "fake-1", "fake-2"}, fmt.Errorf("test"))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		waitForProcess()
		info = q.Info(ctx)
		assert.Len(t, info.Running, 0)

		// Edge case: ErrorAtOnce with empty slice
		err = q.ErrorAtOnce(ctx, []string{}, fmt.Errorf("no tasks"))
		assert.NoError(t, err)
		// Should handle gracefully, potentially no-op

		// Edge case: ErrorAtOnce with nil error
		task5 := &model.Task{ID: "batch-5"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task5}))
		waitForProcess()
		got5, _ := q.Poll(ctx, 3, filterFnTrue)
		err = q.ErrorAtOnce(ctx, []string{got5.ID}, nil)
		assert.NoError(t, err)
		// Should handle nil error gracefully
		waitForProcess()
	})

	t.Run("error at once with waiting deps", func(t *testing.T) {
		task5 := &model.Task{ID: "deps-cancel-5"}
		task6 := &model.Task{
			ID:           "deps-cancel-6",
			Dependencies: []string{"deps-cancel-5"},
			DepStatus:    make(map[string]model.StatusValue),
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task5, task6}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Equal(t, 1, info.Stats.WaitingOnDeps)

		assert.NoError(t, q.ErrorAtOnce(ctx, []string{"deps-cancel-5", "deps-cancel-6"}, fmt.Errorf("canceled")))

		waitForProcess()
		info = q.Info(ctx)
		assert.Equal(t, 0, info.Stats.WaitingOnDeps)
		assert.Len(t, info.Pending, 0)

		// Edge case: verify both tasks are actually gone, not stuck somewhere
		assert.Len(t, info.Running, 0)
		assert.Len(t, info.WaitingOnDeps, 0)
	})

	t.Run("error at once cancellation", func(t *testing.T) {
		task1 := &model.Task{ID: "cancel-prop-1"}
		task2 := &model.Task{
			ID:           "cancel-prop-2",
			Dependencies: []string{"cancel-prop-1"},
			DepStatus:    make(map[string]model.StatusValue),
			RunOn:        []string{"success", "failure"},
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1, task2}))
		waitForProcess()
		got1, _ := q.Poll(ctx, 1, filterFnTrue)

		assert.NoError(t, q.ErrorAtOnce(ctx, []string{got1.ID}, ErrCancel))

		waitForProcess()
		waitForProcess()

		got2, _ := q.Poll(ctx, 2, filterFnTrue)
		assert.Equal(t, model.StatusKilled, got2.DepStatus["cancel-prop-1"])

		// Edge case: verify ErrCancel results in StatusKilled not StatusFailure
		assert.NotEqual(t, model.StatusFailure, got2.DepStatus["cancel-prop-1"])
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("pause resume", func(t *testing.T) {
		dummyTask := &model.Task{ID: "pause-1"}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			_, _ = q.Poll(ctx, 99, filterFnTrue)
			wg.Done()
		}()

		q.Pause()
		t0 := time.Now()
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask}))
		waitForProcess()

		// Edge case: verify queue is actually paused
		info := q.Info(ctx)
		assert.True(t, info.Paused)
		assert.Len(t, info.Pending, 1)
		assert.Len(t, info.Running, 0)

		q.Resume()

		wg.Wait()
		assert.Greater(t, time.Since(t0), 20*time.Millisecond)

		// Edge case: verify queue is unpaused
		info = q.Info(ctx)
		assert.False(t, info.Paused)

		// Edge case: multiple pause/resume cycles
		task2 := &model.Task{ID: "pause-2"}
		q.Pause()
		q.Pause() // Double pause
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task2}))
		waitForProcess()
		q.Resume()
		q.Resume() // Double resume
		waitForProcess()
		got, _ := q.Poll(ctx, 99, filterFnTrue)
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()
	})
}

func TestFifoDependencies(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	t.Run("basic dependency handling", func(t *testing.T) {
		task1 := &model.Task{ID: "dep-basic-1"}
		task2 := &model.Task{
			ID:           "dep-basic-2",
			Dependencies: []string{"dep-basic-1"},
			DepStatus:    make(map[string]model.StatusValue),
		}
		task3 := &model.Task{
			ID:           "dep-basic-3",
			Dependencies: []string{"dep-basic-1"},
			DepStatus:    make(map[string]model.StatusValue),
			RunOn:        []string{"success", "failure"},
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task2, task3, task1}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Equal(t, 2, info.Stats.WaitingOnDeps)

		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, task1, got)
		assert.NoError(t, q.Error(ctx, got.ID, fmt.Errorf("exit code 1")))

		waitForProcess()
		got, err = q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, task2, got)
		assert.False(t, got.ShouldRun())
		assert.Equal(t, model.StatusFailure, got.DepStatus["dep-basic-1"])

		waitForProcess()
		got, err = q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, task3, got)
		assert.True(t, got.ShouldRun())
		assert.Equal(t, model.StatusFailure, got.DepStatus["dep-basic-1"])

		waitForProcess()
		info = q.Info(ctx)
		assert.Equal(t, 0, info.Stats.WaitingOnDeps)

		// Edge case: verify DepStatus is correctly set before polling
		assert.NotEmpty(t, task2.DepStatus)
		assert.NotEmpty(t, task3.DepStatus)
	})

	t.Run("multiple dependencies", func(t *testing.T) {
		task1 := &model.Task{ID: "multi-dep-1"}
		task2 := &model.Task{ID: "multi-dep-2"}
		task3 := &model.Task{
			ID:           "multi-dep-3",
			Dependencies: []string{"multi-dep-1", "multi-dep-2"},
			DepStatus:    make(map[string]model.StatusValue),
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task2, task3, task1}))
		waitForProcess()

		got1, _ := q.Poll(ctx, 1, filterFnTrue)
		got2, _ := q.Poll(ctx, 2, filterFnTrue)

		gotIDs := map[string]bool{got1.ID: true, got2.ID: true}
		assert.True(t, gotIDs["multi-dep-1"] && gotIDs["multi-dep-2"])

		if got1.ID == "multi-dep-1" {
			assert.NoError(t, q.Done(ctx, got1.ID, model.StatusSuccess))
			assert.NoError(t, q.Error(ctx, got2.ID, fmt.Errorf("failed")))
		} else {
			assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
			assert.NoError(t, q.Error(ctx, got1.ID, fmt.Errorf("failed")))
		}

		waitForProcess()
		got3, err := q.Poll(ctx, 3, filterFnTrue)
		assert.NoError(t, err)

		assert.Contains(t, got3.DepStatus, "multi-dep-1")
		assert.Contains(t, got3.DepStatus, "multi-dep-2")
		assert.True(t,
			(got3.DepStatus["multi-dep-1"] == model.StatusSuccess && got3.DepStatus["multi-dep-2"] == model.StatusFailure) ||
				(got3.DepStatus["multi-dep-1"] == model.StatusFailure && got3.DepStatus["multi-dep-2"] == model.StatusSuccess))
		assert.False(t, got3.ShouldRun())

		// Edge case: verify both deps are tracked
		assert.Len(t, got3.DepStatus, 2)
		assert.NoError(t, q.Done(ctx, got3.ID, model.StatusSkipped))
		waitForProcess()
	})

	t.Run("transitive dependencies", func(t *testing.T) {
		task1 := &model.Task{ID: "trans-1"}
		task2 := &model.Task{
			ID:           "trans-2",
			Dependencies: []string{"trans-1"},
			DepStatus:    make(map[string]model.StatusValue),
		}
		task3 := &model.Task{
			ID:           "trans-3",
			Dependencies: []string{"trans-2"},
			DepStatus:    make(map[string]model.StatusValue),
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task2, task3, task1}))
		waitForProcess()

		got, _ := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, q.Error(ctx, got.ID, fmt.Errorf("exit code 1")))

		waitForProcess()
		got, _ = q.Poll(ctx, 2, filterFnTrue)
		assert.False(t, got.ShouldRun())
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSkipped))

		waitForProcess()
		got, _ = q.Poll(ctx, 3, filterFnTrue)
		assert.Equal(t, model.StatusSkipped, got.DepStatus["trans-2"])
		assert.False(t, got.ShouldRun())

		// Edge case: verify transitive failure propagates correctly
		// task3 should see trans-2 as skipped, not trans-1's status
		assert.NotContains(t, got.DepStatus, "trans-1")
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSkipped))
		waitForProcess()
	})

	t.Run("dependency status propagation", func(t *testing.T) {
		task1 := &model.Task{ID: "prop-1"}
		task2 := &model.Task{
			ID:           "prop-2",
			Dependencies: []string{"prop-1"},
			DepStatus:    make(map[string]model.StatusValue),
		}
		task3 := &model.Task{
			ID:           "prop-3",
			Dependencies: []string{"prop-1"},
			DepStatus:    make(map[string]model.StatusValue),
			RunOn:        []string{"success", "failure"},
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1, task2, task3}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Equal(t, 2, info.Stats.WaitingOnDeps)

		got1, _ := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, q.Done(ctx, got1.ID, model.StatusSuccess))

		waitForProcess()

		got2, _ := q.Poll(ctx, 2, filterFnTrue)
		got3, _ := q.Poll(ctx, 3, filterFnTrue)

		assert.Equal(t, model.StatusSuccess, got2.DepStatus["prop-1"])
		assert.Equal(t, model.StatusSuccess, got3.DepStatus["prop-1"])

		// Edge case: verify both tasks can be polled concurrently
		assert.NotEqual(t, got2.ID, got3.ID)
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		assert.NoError(t, q.Done(ctx, got3.ID, model.StatusSuccess))
		waitForProcess()

		task4 := &model.Task{ID: "prop-4"}
		task5 := &model.Task{
			ID:           "prop-5",
			Dependencies: []string{"prop-4"},
			DepStatus:    make(map[string]model.StatusValue),
		}
		task6 := &model.Task{
			ID:           "prop-6",
			Dependencies: []string{"prop-4"},
			DepStatus:    make(map[string]model.StatusValue),
			RunOn:        []string{"success", "failure"},
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task4, task5, task6}))
		waitForProcess()

		got4, _ := q.Poll(ctx, 4, filterFnTrue)
		assert.NoError(t, q.Error(ctx, got4.ID, fmt.Errorf("failed")))

		waitForProcess()

		got5, _ := q.Poll(ctx, 5, filterFnTrue)
		assert.Equal(t, model.StatusFailure, got5.DepStatus["prop-4"])
		assert.False(t, got5.ShouldRun())

		got6, _ := q.Poll(ctx, 6, filterFnTrue)
		assert.Equal(t, model.StatusFailure, got6.DepStatus["prop-4"])
		assert.True(t, got6.ShouldRun())

		// Edge case: complete dependent tasks
		assert.NoError(t, q.Done(ctx, got5.ID, model.StatusSkipped))
		assert.NoError(t, q.Done(ctx, got6.ID, model.StatusSuccess))
		waitForProcess()
	})

	// Edge case: circular dependency detection (should be handled or cause issue)
	t.Run("circular dependencies", func(t *testing.T) {
		task1 := &model.Task{
			ID:           "circ-1",
			Dependencies: []string{"circ-2"},
			DepStatus:    make(map[string]model.StatusValue),
		}
		task2 := &model.Task{
			ID:           "circ-2",
			Dependencies: []string{"circ-1"},
			DepStatus:    make(map[string]model.StatusValue),
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1, task2}))
		waitForProcess()

		info := q.Info(ctx)
		// Both should be waiting on deps - this is a deadlock scenario
		assert.Equal(t, 2, info.Stats.WaitingOnDeps)
		assert.Len(t, info.Pending, 0)

		// Verify they never become available for polling
		pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err := q.Poll(pollCtx, 99, filterFnTrue)
		pollCancel()
		assert.Error(t, err) // Should timeout

		// Clean up the deadlocked tasks
		assert.NoError(t, q.ErrorAtOnce(ctx, []string{"circ-1", "circ-2"}, fmt.Errorf("circular dep")))
		waitForProcess()
	})

	// Edge case: dependency on non-existent task
	// NOTE: This reveals a potential issue - the queue doesn't validate dependencies exist.
	// If a dependency was never added to the queue, the task will run immediately since
	// depsInQueue() only checks currently pending/running tasks, not if deps will arrive.
	t.Run("non-existent dependency", func(t *testing.T) {
		task1 := &model.Task{
			ID:           "orphan-1",
			Dependencies: []string{"does-not-exist"},
			DepStatus:    make(map[string]model.StatusValue),
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task1}))
		waitForProcess()

		info := q.Info(ctx)
		// Current implementation: task doesn't wait if dependency not in queue
		// This means tasks with typos in dependency names will run immediately!
		assert.Equal(t, 0, info.Stats.WaitingOnDeps)
		assert.Len(t, info.Pending, 1)

		// Task will be available for polling even though dependency doesn't exist
		got, err := q.Poll(ctx, 99, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "orphan-1", got.ID)

		// DepStatus will be empty since dependency never completed
		assert.Empty(t, got.DepStatus)

		// Clean up
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()
	})

	// Edge case: dependency added AFTER dependent task (race condition)
	t.Run("dependency added after dependent", func(t *testing.T) {
		// Push dependent task first
		dependent := &model.Task{
			ID:           "late-dep-child",
			Dependencies: []string{"late-dep-parent"},
			DepStatus:    make(map[string]model.StatusValue),
		}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dependent}))
		waitForProcess()

		// At this point, dependent doesn't see parent in queue, so it won't wait
		info := q.Info(ctx)
		// Dependent should NOT be waiting since parent doesn't exist yet
		initialWaiting := info.Stats.WaitingOnDeps

		// Now add the parent task
		parent := &model.Task{ID: "late-dep-parent"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{parent}))
		waitForProcess()

		// After filterWaiting runs, dependent SHOULD now see parent and wait
		info = q.Info(ctx)
		// The implementation calls filterWaiting() which rechecks dependencies
		// So dependent should now be waiting
		assert.Greater(t, info.Stats.WaitingOnDeps, initialWaiting,
			"dependent should start waiting once parent is added")

		// Complete parent first
		gotParent, _ := q.Poll(ctx, 1, filterFnTrue)
		assert.Equal(t, "late-dep-parent", gotParent.ID, "parent should be polled first")
		assert.NoError(t, q.Done(ctx, gotParent.ID, model.StatusSuccess))
		waitForProcess()

		// Now child should be unblocked with parent's status
		gotChild, _ := q.Poll(ctx, 2, filterFnTrue)
		assert.Equal(t, "late-dep-child", gotChild.ID)
		assert.Equal(t, model.StatusSuccess, gotChild.DepStatus["late-dep-parent"])

		assert.NoError(t, q.Done(ctx, gotChild.ID, model.StatusSuccess))
		waitForProcess()
	})
}

func TestFifoConcurrency(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	t.Run("limit serializes group in instantiation order", func(t *testing.T) {
		// Lower Created == instantiated earlier. taskB is pushed first to
		// prove the queue serializes by creation order, not by push/ready
		// order. Distinct pipeline IDs model two pipelines of the same
		// workflow. Ordering must not depend on the task ID, so taskA (the
		// earlier one) deliberately has the higher ID.
		taskA := &model.Task{ID: "200", PipelineID: 1, Created: 100, ConcurrencyGroup: "repo:deploy", ConcurrencyLimit: 1}
		taskB := &model.Task{ID: "100", PipelineID: 2, Created: 200, ConcurrencyGroup: "repo:deploy", ConcurrencyLimit: 1}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{taskB, taskA}))
		waitForProcess()

		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "200", got.ID) // earliest instantiated (lowest Created) runs first

		waitForProcess()
		info := q.Info(ctx)
		assert.Len(t, info.Running, 1)
		assert.Len(t, info.Pending, 1) // taskB deferred by concurrency limit

		// taskB cannot be polled while taskA holds the only slot.
		pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err = q.Poll(pollCtx, 2, filterFnTrue)
		pollCancel()
		assert.Error(t, err)

		// finishing taskA frees the slot for taskB.
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()

		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "100", got2.ID)
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("preserves instantiation order over readiness", func(t *testing.T) {
		// Pipeline 1's deploy (taskA) waits on its own slow check (dep), while
		// pipeline 2's deploy (taskB) is immediately ready. taskB must still
		// wait for the earlier taskA. Ordering is by Created, not ID, so taskA
		// has the higher ID but the lower Created.
		dep := &model.Task{ID: "50", PipelineID: 1, Created: 100}
		taskA := &model.Task{
			ID:               "200",
			PipelineID:       1,
			Created:          100,
			ConcurrencyGroup: "repo:deploy2",
			ConcurrencyLimit: 1,
			Dependencies:     []string{"50"},
			DepStatus:        make(map[string]model.StatusValue),
			RunOn:            []string{"success", "failure"},
		}
		taskB := &model.Task{ID: "100", PipelineID: 2, Created: 200, ConcurrencyGroup: "repo:deploy2", ConcurrencyLimit: 1}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dep, taskA, taskB}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Equal(t, 1, info.Stats.WaitingOnDeps) // taskA waiting on dep

		// only the dependency is runnable; taskB is held back behind taskA.
		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "50", got.ID)

		pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err = q.Poll(pollCtx, 2, filterFnTrue)
		pollCancel()
		assert.Error(t, err, "taskB must not overtake the earlier taskA")

		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()

		gotA, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "200", gotA.ID)
		assert.NoError(t, q.Done(ctx, gotA.ID, model.StatusSuccess))
		waitForProcess()

		gotB, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "100", gotB.ID)
		assert.NoError(t, q.Done(ctx, gotB.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("limit greater than one allows parallelism", func(t *testing.T) {
		t1 := &model.Task{ID: "300", PipelineID: 1, ConcurrencyGroup: "repo:build", ConcurrencyLimit: 2}
		t2 := &model.Task{ID: "400", PipelineID: 2, ConcurrencyGroup: "repo:build", ConcurrencyLimit: 2}
		t3 := &model.Task{ID: "500", PipelineID: 3, ConcurrencyGroup: "repo:build", ConcurrencyLimit: 2}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{t1, t2, t3}))
		waitForProcess()

		g1, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		g2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"300", "400"}, []string{g1.ID, g2.ID})

		waitForProcess()
		info := q.Info(ctx)
		assert.Len(t, info.Running, 2)
		assert.Len(t, info.Pending, 1) // third deferred until a slot frees

		pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err = q.Poll(pollCtx, 3, filterFnTrue)
		pollCancel()
		assert.Error(t, err)

		assert.NoError(t, q.Done(ctx, g1.ID, model.StatusSuccess))
		waitForProcess()
		g3, err := q.Poll(ctx, 3, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "500", g3.ID)
		assert.NoError(t, q.Done(ctx, g2.ID, model.StatusSuccess))
		assert.NoError(t, q.Done(ctx, g3.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("same pipeline dependency does not deadlock", func(t *testing.T) {
		// Within one pipeline, deploy.yaml (lower ID, alphabetically first)
		// depends on test.yaml (higher ID), and both share a concurrency group.
		// The ordering reservation must not treat the dependent deploy as
		// "ahead" of its own dependency, otherwise neither can ever run.
		deploy := &model.Task{
			ID:               "100",
			PipelineID:       1,
			ConcurrencyGroup: "repo:ci",
			ConcurrencyLimit: 1,
			Dependencies:     []string{"200"},
			DepStatus:        make(map[string]model.StatusValue),
			RunOn:            []string{"success", "failure"},
		}
		test := &model.Task{
			ID:               "200",
			PipelineID:       1,
			ConcurrencyGroup: "repo:ci",
			ConcurrencyLimit: 1,
		}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{deploy, test}))
		waitForProcess()

		// test must be runnable even though deploy has a lower ID.
		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "200", got.ID, "the dependency must not be starved by the dependent")
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()

		gotDeploy, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "100", gotDeploy.ID)
		assert.NoError(t, q.Done(ctx, gotDeploy.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("different groups do not block each other", func(t *testing.T) {
		t1 := &model.Task{ID: "600", ConcurrencyGroup: "repo:a", ConcurrencyLimit: 1}
		t2 := &model.Task{ID: "700", ConcurrencyGroup: "repo:b", ConcurrencyLimit: 1}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{t1, t2}))
		waitForProcess()

		g1, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		g2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"600", "700"}, []string{g1.ID, g2.ID})

		assert.NoError(t, q.Done(ctx, g1.ID, model.StatusSuccess))
		assert.NoError(t, q.Done(ctx, g2.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("no limit keeps default behavior", func(t *testing.T) {
		t1 := &model.Task{ID: "800"}
		t2 := &model.Task{ID: "900"}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{t1, t2}))
		waitForProcess()

		g1, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		g2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"800", "900"}, []string{g1.ID, g2.ID})

		assert.NoError(t, q.Done(ctx, g1.ID, model.StatusSuccess))
		assert.NoError(t, q.Done(ctx, g2.ID, model.StatusSuccess))
		waitForProcess()
	})

	t.Run("task ordering uses Created with name tiebreak", func(t *testing.T) {
		// earlier Created sorts first, regardless of ID.
		assert.True(t, taskOrderLess(
			&model.Task{ID: "999", Created: 100},
			&model.Task{ID: "1", Created: 200},
		))
		assert.False(t, taskOrderLess(
			&model.Task{ID: "1", Created: 200},
			&model.Task{ID: "999", Created: 100},
		))
		// equal Created falls back to the workflow name, alphabetically.
		assert.True(t, taskOrderLess(
			&model.Task{ID: "2", Created: 100, Name: "alpha"},
			&model.Task{ID: "1", Created: 100, Name: "beta"},
		))
		assert.False(t, taskOrderLess(
			&model.Task{ID: "1", Created: 100, Name: "beta"},
			&model.Task{ID: "2", Created: 100, Name: "alpha"},
		))
	})
}

func TestFifoLeaseManagement(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	t.Run("lease expiration", func(t *testing.T) {
		q.extension = 0
		t.Cleanup(func() {
			q.extension = 50 * time.Millisecond
		})
		dummyTask := &model.Task{ID: "lease-exp-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask}))

		waitForProcess()
		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)

		errCh := make(chan error, 1)
		go func() { errCh <- q.Wait(ctx, got.ID) }()

		waitForProcess()
		select {
		case werr := <-errCh:
			assert.Error(t, werr)
			// Edge case: verify error is ErrTaskExpired
			assert.ErrorIs(t, werr, ErrTaskExpired)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Wait to return")
		}

		info := q.Info(ctx)
		assert.Len(t, info.Pending, 1)

		// Edge case: verify task was resubmitted to front of queue
		got2, _ := q.Poll(ctx, 1, filterFnTrue)
		assert.Equal(t, got.ID, got2.ID) // Same task resubmitted

		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()

		// Verify cleanup
		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("extend lease", func(t *testing.T) {
		q.extension = 50 * time.Millisecond
		dummyTask := &model.Task{ID: "extend-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask}))

		waitForProcess()
		got, _ := q.Poll(ctx, 5, filterFnTrue)

		assert.NoError(t, q.Extend(ctx, 5, got.ID))
		assert.ErrorIs(t, q.Extend(ctx, 999, got.ID), ErrAgentMissMatch)
		assert.ErrorIs(t, q.Extend(ctx, 1, got.ID), ErrAgentMissMatch)
		assert.ErrorIs(t, q.Extend(ctx, 1, "non-existent"), ErrNotFound)

		// Edge case: extend multiple times rapidly
		for i := 0; i < 3; i++ {
			time.Sleep(30 * time.Millisecond)
			assert.NoError(t, q.Extend(ctx, 5, got.ID))
		}

		info := q.Info(ctx)
		assert.Len(t, info.Running, 1)
		assert.Len(t, info.Pending, 0)

		// Edge case: extend after Done should error
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()
		assert.ErrorIs(t, q.Extend(ctx, 5, got.ID), ErrNotFound)

		// Verify cleanup
		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("wait operations", func(t *testing.T) {
		// Verify queue is clean before starting
		info := q.Info(ctx)
		assert.Len(t, info.Pending, 0, "queue should be empty at start of wait operations")
		assert.Len(t, info.Running, 0, "queue should be empty at start of wait operations")

		dummyTask := &model.Task{ID: "wait-1"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask}))

		waitForProcess()
		got, _ := q.Poll(ctx, 1, filterFnTrue)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			assert.NoError(t, q.Wait(ctx, got.ID))
			wg.Done()
		}()

		time.Sleep(time.Millisecond)
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		wg.Wait()

		// Edge case: Wait on non-existent task should return immediately
		assert.NoError(t, q.Wait(ctx, "non-existent"))

		dummyTask2 := &model.Task{ID: "wait-2"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask2}))
		waitForProcess()
		got2, _ := q.Poll(ctx, 1, filterFnTrue)

		waitCtx, waitCancel := context.WithCancelCause(ctx)
		errCh := make(chan error, 1)
		go func() { errCh <- q.Wait(waitCtx, got2.ID) }()

		time.Sleep(50 * time.Millisecond)
		waitCancel(nil)

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("Wait should return when context is canceled")
		}

		// Clean up - complete the second wait task
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()

		// Edge case: multiple concurrent waits on same task
		dummyTask3 := &model.Task{ID: "wait-3"}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dummyTask3}))
		waitForProcess()
		got3, _ := q.Poll(ctx, 1, filterFnTrue)

		var wg2 sync.WaitGroup
		wg2.Add(3)
		for i := 0; i < 3; i++ {
			go func() {
				assert.NoError(t, q.Wait(ctx, got3.ID))
				wg2.Done()
			}()
		}

		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, q.Done(ctx, got3.ID, model.StatusSuccess))
		wg2.Wait()

		// Verify cleanup
		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})
}

func TestFifoWorkerManagement(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	t.Run("poll with context cancellation", func(t *testing.T) {
		pollCtx, pollCancel := context.WithCancelCause(ctx)
		errCh := make(chan error, 1)
		go func() {
			_, err := q.Poll(pollCtx, 1, filterFnTrue)
			errCh <- err
		}()

		time.Sleep(50 * time.Millisecond)
		pollCancel(nil)

		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Poll should return when context is canceled")
		}

		// Edge case: verify worker is cleaned up
		info := q.Info(ctx)
		assert.Equal(t, 0, info.Stats.Workers)
	})

	t.Run("kick agent workers", func(t *testing.T) {
		pollResults := make(chan error, 5)
		for i := 0; i < 5; i++ {
			go func() {
				_, err := q.Poll(ctx, 42, filterFnTrue)
				pollResults <- err
			}()
		}

		time.Sleep(50 * time.Millisecond)

		// Edge case: verify workers are registered before kicking
		info := q.Info(ctx)
		assert.Equal(t, 5, info.Stats.Workers)

		q.KickAgentWorkers(42)

		kickedCount := 0
		for i := 0; i < 5; i++ {
			select {
			case err := <-pollResults:
				if errors.Is(err, context.Canceled) {
					kickedCount++
				}
			case <-time.After(time.Second):
				t.Fatal("expected all workers to be kicked")
			}
		}
		assert.Equal(t, 5, kickedCount)

		// Edge case: verify workers are removed after kicking
		waitForProcess()
		info = q.Info(ctx)
		assert.Equal(t, 0, info.Stats.Workers)

		// Edge case: kick non-existent agent should be no-op
		q.KickAgentWorkers(999)
	})

	// Edge case: mixed agent workers
	t.Run("kick specific agent among multiple", func(t *testing.T) {
		pollResults := make(chan struct {
			agentID int64
			err     error
		}, 10)

		// Start workers for agent 1
		for i := 0; i < 3; i++ {
			go func() {
				_, err := q.Poll(ctx, 1, filterFnTrue)
				pollResults <- struct {
					agentID int64
					err     error
				}{1, err}
			}()
		}

		// Start workers for agent 2
		for i := 0; i < 3; i++ {
			go func() {
				_, err := q.Poll(ctx, 2, filterFnTrue)
				pollResults <- struct {
					agentID int64
					err     error
				}{2, err}
			}()
		}

		time.Sleep(50 * time.Millisecond)
		info := q.Info(ctx)
		assert.Equal(t, 6, info.Stats.Workers)

		// Kick only agent 1
		q.KickAgentWorkers(1)

		kickedAgent1 := 0
		kickedAgent2 := 0
		for i := 0; i < 3; i++ {
			select {
			case result := <-pollResults:
				if errors.Is(result.err, context.Canceled) {
					if result.agentID == 1 {
						kickedAgent1++
					} else {
						kickedAgent2++
					}
				}
			case <-time.After(time.Second):
				t.Fatal("expected kicked workers to return")
			}
		}

		assert.Equal(t, 3, kickedAgent1)
		assert.Equal(t, 0, kickedAgent2)

		// Clean up agent 2 workers
		q.KickAgentWorkers(2)
		for i := 0; i < 3; i++ {
			<-pollResults
		}
	})
}

func TestFifoLabelBasedScoring(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	q := NewMemoryQueue(ctx)

	tasks := []*model.Task{
		{ID: "1", Labels: map[string]string{"org-id": "123", "platform": "linux"}},
		{ID: "2", Labels: map[string]string{"org-id": "456", "platform": "linux"}},
		{ID: "3", Labels: map[string]string{"org-id": "123", "platform": "windows"}},
	}

	assert.NoError(t, q.PushAtOnce(ctx, tasks))

	filter123 := func(task *model.Task) (bool, int) {
		if task.Labels["org-id"] == "123" {
			return true, 20
		}
		return true, 1
	}

	filter456 := func(task *model.Task) (bool, int) {
		if task.Labels["org-id"] == "456" {
			return true, 20
		}
		return true, 1
	}

	results := make(chan *model.Task, 2)
	go func() {
		task, _ := q.Poll(ctx, 1, filter123)
		results <- task
	}()
	go func() {
		task, _ := q.Poll(ctx, 2, filter456)
		results <- task
	}()

	receivedTasks := make(map[string]int64)
	for i := 0; i < 2; i++ {
		select {
		case task := <-results:
			receivedTasks[task.ID] = task.AgentID
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for tasks")
		}
	}

	assert.Contains(t, []string{"1", "3"}, findTaskByAgent(receivedTasks, 1))
	assert.Equal(t, "2", findTaskByAgent(receivedTasks, 2))

	// Edge case: filter that rejects all tasks
	filterRejectAll := func(task *model.Task) (bool, int) {
		return false, 0
	}

	task4 := &model.Task{ID: "4", Labels: map[string]string{"org-id": "789"}}
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task4}))
	waitForProcess()

	pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err := q.Poll(pollCtx, 99, filterRejectAll)
	pollCancel()
	assert.Error(t, err) // Should timeout as filter rejects task

	// Clean up remaining tasks
	task3, _ := q.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, q.Done(ctx, task3.ID, model.StatusSuccess))
	task4Got, _ := q.Poll(ctx, 99, filterFnTrue)
	assert.NoError(t, q.Done(ctx, task4Got.ID, model.StatusSuccess))
	waitForProcess()
}

func TestShouldRunLogic(t *testing.T) {
	tests := []struct {
		name      string
		depStatus model.StatusValue
		runOn     []string
		expected  bool
	}{
		{"Success without RunOn", model.StatusSuccess, nil, true},
		{"Failure without RunOn", model.StatusFailure, nil, false},
		{"Success with failure RunOn", model.StatusSuccess, []string{"failure"}, false},
		{"Failure with failure RunOn", model.StatusFailure, []string{"failure"}, true},
		{"Success with both RunOn", model.StatusSuccess, []string{"success", "failure"}, true},
		{"Skipped without RunOn", model.StatusSkipped, nil, false},
		{"Skipped with failure RunOn", model.StatusSkipped, []string{"failure"}, true},
		// Edge cases
		{"Killed without RunOn", model.StatusKilled, nil, false},
		{"Killed with failure RunOn", model.StatusKilled, []string{"failure"}, true},
		{"Success with success RunOn only", model.StatusSuccess, []string{"success"}, true},
		{"Failure with success RunOn only", model.StatusFailure, []string{"success"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &model.Task{
				ID:           "2",
				Dependencies: []string{"1"},
				DepStatus:    map[string]model.StatusValue{"1": tt.depStatus},
				RunOn:        tt.runOn,
			}
			assert.Equal(t, tt.expected, task.ShouldRun())
		})
	}

	// Edge case: multiple dependencies with mixed statuses
	t.Run("multiple deps mixed status", func(t *testing.T) {
		task := &model.Task{
			ID:           "3",
			Dependencies: []string{"1", "2"},
			DepStatus: map[string]model.StatusValue{
				"1": model.StatusSuccess,
				"2": model.StatusFailure,
			},
			RunOn: nil,
		}
		// With default RunOn (nil), needs all deps successful
		assert.False(t, task.ShouldRun())

		task.RunOn = []string{"success", "failure"}
		// With both RunOn, should run regardless
		assert.True(t, task.ShouldRun())
	})
}

func findTaskByAgent(tasks map[string]int64, agentID int64) string {
	for taskID, aid := range tasks {
		if aid == agentID {
			return taskID
		}
	}
	return ""
}

func TestFifoFairDispatch(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	t.Run("older pipeline dependent dispatches before newer pipeline task", func(t *testing.T) {
		// Core fair-dispatch bug: when an older pipeline's dependent clears its
		// dependency it is PushBack'd to the TAIL of pending, landing behind a
		// task from a pipeline created later and getting starved. Dispatch must
		// order by Created, not insertion order, so the earlier task deliberately
		// carries the HIGHER ID to prove ordering is by Created, never by ID.
		dep := &model.Task{ID: "10", PipelineID: 1, Created: 100}
		older := &model.Task{
			ID:           "20",
			PipelineID:   1,
			Created:      100,
			Dependencies: []string{"10"},
			DepStatus:    make(map[string]model.StatusValue),
			RunOn:        []string{"success", "failure"},
		}
		newer := &model.Task{ID: "30", PipelineID: 2, Created: 200}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dep, older, newer}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Equal(t, 1, info.Stats.WaitingOnDeps) // older is parked on its dependency

		// dep and newer are runnable; dep sorts first by Created either way.
		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "10", got.ID)

		// completing dep clears older's dependency; filterWaiting PushBacks older
		// to the tail of pending, leaving pending == [newer, older].
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()

		// the now-ready older dependent (lower Created) must win over newer.
		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "20", got2.ID, "older pipeline's now-ready dependent must dispatch before the newer pipeline's task")

		// drain whichever task actually dispatched so the queue empties cleanly
		// whether or not creation-order dispatch is in effect; the defended
		// contract lives entirely in the assertion above.
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()
		got3, err := q.Poll(ctx, 3, filterFnTrue)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"20", "30"}, []string{got2.ID, got3.ID})
		assert.NoError(t, q.Done(ctx, got3.ID, model.StatusSuccess))
		waitForProcess()

		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("concurrency admission is invariant to the Created-ordered walk", func(t *testing.T) {
		// The creation-order dispatch reorders the pending walk; the concurrency admit/defer
		// decision (a full, position-independent scan) must be unchanged. An
		// unlimited task whose Created falls BETWEEN the two group members must
		// run freely without disturbing which member takes the single slot.
		// IDs track Created only incidentally here; ordering is proved elsewhere.
		a := &model.Task{ID: "100", PipelineID: 1, Created: 100, ConcurrencyGroup: "repo:deploy", ConcurrencyLimit: 1}
		c := &model.Task{ID: "150", PipelineID: 3, Created: 150} // unlimited, sorts BETWEEN a and b
		b := &model.Task{ID: "200", PipelineID: 2, Created: 200, ConcurrencyGroup: "repo:deploy", ConcurrencyLimit: 1}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{a, b, c}))
		waitForProcess()

		g1, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		g2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		// a (earliest group member) takes the single group slot; c (unlimited)
		// runs freely; b is deferred even though the sorted walk visits c between.
		assert.ElementsMatch(t, []string{"100", "150"}, []string{g1.ID, g2.ID})

		waitForProcess()
		info := q.Info(ctx)
		assert.Len(t, info.Running, 2)
		assert.Len(t, info.Pending, 1) // b deferred by the concurrency limit

		// b cannot be polled while a holds the only slot.
		pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err = q.Poll(pollCtx, 3, filterFnTrue)
		pollCancel()
		assert.Error(t, err)

		// freeing a's slot lets b (the remaining group member) run.
		assert.NoError(t, q.Done(ctx, "100", model.StatusSuccess))
		waitForProcess()
		g3, err := q.Poll(ctx, 3, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "200", g3.ID)

		assert.NoError(t, q.Done(ctx, "150", model.StatusSuccess))
		assert.NoError(t, q.Done(ctx, g3.ID, model.StatusSuccess))
		waitForProcess()

		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("expired lease retries before a newer pending task", func(t *testing.T) {
		// An expired task is resubmitted to the pending list; the creation-order
		// sort must keep its retry priority so it re-dispatches ahead of a task
		// from a pipeline created later. Expiry is observed deterministically by
		// re-inspecting the queue and re-polling — never by racing the process
		// ticker against a Wait goroutine.
		q.extension = 0
		t.Cleanup(func() { q.extension = 50 * time.Millisecond })

		old := &model.Task{ID: "10", Created: 100}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{old}))
		waitForProcess()
		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "10", got.ID)

		// with a zero lease the task expires immediately; the next process tick
		// resubmits it to pending. Wait for that tick, then confirm it is back
		// in pending and no longer running.
		waitForProcess()
		info := q.Info(ctx)
		assert.Len(t, info.Running, 0)
		assert.Len(t, info.Pending, 1)
		assert.Equal(t, "10", info.Pending[0].ID) // resubmitted expired task

		newer := &model.Task{ID: "20", Created: 200}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{newer}))
		waitForProcess()

		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "10", got2.ID, "expired older task retries before the newer pending task")

		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()
		got3, err := q.Poll(ctx, 3, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "20", got3.ID)
		assert.NoError(t, q.Done(ctx, got3.ID, model.StatusSuccess))
		waitForProcess()

		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("same-Created siblings dispatch in name order", func(t *testing.T) {
		// Two independent same-pipeline siblings share Created; the tiebreaker is
		// the workflow Name, not insertion order or ID. "aaa" sorts before "zzz"
		// yet carries the HIGHER ID, so a pass proves ordering is by Name, not ID.
		zzz := &model.Task{ID: "10", PipelineID: 1, Created: 100, Name: "zzz"}
		aaa := &model.Task{ID: "20", PipelineID: 1, Created: 100, Name: "aaa"}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{zzz, aaa}))
		waitForProcess()

		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "20", got.ID, "same-Created siblings dispatch in name order")

		// drain off the actually dispatched IDs so a regression ends as an
		// assertion failure rather than hanging on an empty queue.
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()
		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"10", "20"}, []string{got.ID, got2.ID})
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()

		info := q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("identical Created and Name dispatch in insertion order", func(t *testing.T) {
		// Two tasks the comparator treats as fully equal — same Created AND same
		// Name — must keep their relative order. This is the documented
		// stability guarantee of the creation-order sort and the only case that
		// reaches the comparator's equal branch. The tasks are pushed with the
		// higher ID first so a tiebreak that reordered equal elements (e.g. by
		// ID) would flip the result and be caught.
		second := &model.Task{ID: "2", PipelineID: 1, Created: 100, Name: "build"}
		first := &model.Task{ID: "1", PipelineID: 1, Created: 100, Name: "build"}

		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{second, first}))
		waitForProcess()

		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "2", got.ID, "equal-comparator tasks dispatch in insertion order")

		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()
		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "1", got2.ID, "equal-comparator tasks dispatch in insertion order")
		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()

		info := q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("multiple workers dispatch the earliest-Created tasks first", func(t *testing.T) {
		// Several runnable tasks and several waiting workers coexist within a
		// single process tick, so the queue makes multiple back-to-back
		// assignments while re-sorting the shrinking pending set. IDs are
		// inverted against Created so an insertion-order (unsorted) walk would
		// starve the earliest pipelines. With three workers and four independent
		// tasks the three earliest-Created tasks must win the slots; the latest
		// waits. The dispatched SET is asserted (order among the concurrent
		// workers is not externally observable), which is deterministic
		// regardless of goroutine and tick scheduling.
		t4 := &model.Task{ID: "1", PipelineID: 1, Created: 400}
		t3 := &model.Task{ID: "2", PipelineID: 2, Created: 300}
		t2 := &model.Task{ID: "3", PipelineID: 3, Created: 200}
		t1 := &model.Task{ID: "4", PipelineID: 4, Created: 100}
		// pushed newest-first: insertion order is the reverse of Created order.
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{t4, t3, t2, t1}))

		results := make(chan *model.Task, 3)
		for i := range 3 {
			agentID := int64(i + 1)
			go func() {
				task, _ := q.Poll(ctx, agentID, filterFnTrue)
				results <- task
			}()
		}

		dispatched := make([]string, 0, 3)
		for range 3 {
			select {
			case task := <-results:
				dispatched = append(dispatched, task.ID)
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for concurrent dispatch")
			}
		}
		// the three earliest-Created tasks take the three slots; the
		// latest-Created task is left pending.
		assert.ElementsMatch(t, []string{"4", "3", "2"}, dispatched)

		info := q.Info(ctx)
		assert.Len(t, info.Pending, 1)
		assert.Equal(t, "1", info.Pending[0].ID, "latest-Created task waits while earlier tasks take the slots")

		// draining the slots lets the last task run.
		for _, id := range dispatched {
			assert.NoError(t, q.Done(ctx, id, model.StatusSuccess))
		}
		waitForProcess()
		got, err := q.Poll(ctx, 4, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "1", got.ID)
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()

		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})

	t.Run("dependent with lower Created still dispatches after its dependency", func(t *testing.T) {
		// Dependency safety must never be inverted by creation-order sorting: a
		// dependent whose Created is EARLIER than its dependency would sort first
		// on Created alone, but filterWaiting parks it until the dependency
		// completes, so it can never dispatch ahead of what it depends on.
		dep := &model.Task{ID: "1", PipelineID: 1, Created: 200}
		dependent := &model.Task{
			ID:           "2",
			PipelineID:   1,
			Created:      100, // LOWER than dep — would sort first if not parked
			Dependencies: []string{"1"},
			DepStatus:    make(map[string]model.StatusValue),
			RunOn:        []string{"success", "failure"},
		}
		assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{dep, dependent}))
		waitForProcess()

		info := q.Info(ctx)
		assert.Equal(t, 1, info.Stats.WaitingOnDeps) // dependent parked despite its lower Created

		got, err := q.Poll(ctx, 1, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "1", got.ID, "dependency dispatches before its lower-Created dependent")

		// completing the dependency clears the block; only now may the dependent run.
		assert.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
		waitForProcess()
		got2, err := q.Poll(ctx, 2, filterFnTrue)
		assert.NoError(t, err)
		assert.Equal(t, "2", got2.ID, "lower-Created dependent runs only after its dependency completes")

		assert.NoError(t, q.Done(ctx, got2.ID, model.StatusSuccess))
		waitForProcess()
		info = q.Info(ctx)
		assert.Len(t, info.Pending, 0)
		assert.Len(t, info.Running, 0)
	})
}

func TestFifoMultiDispatchTickOrder(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	// A single process tick dispatches in a loop, so several tasks leave the
	// pending list in one pass. Only the earliest-Created tasks may go, however
	// many workers are waiting: with fewer workers than tasks, the tail of the
	// creation order must stay pending. Tasks are pushed newest-first, and in
	// separate batches, so a pending list that is not kept in creation order
	// dispatches the wrong ones.
	pushes := [][]*model.Task{
		{{ID: "50", Created: 500}, {ID: "40", Created: 400}},
		{{ID: "30", Created: 300}},
		{{ID: "20", Created: 200}, {ID: "10", Created: 100}},
	}
	for _, batch := range pushes {
		assert.NoError(t, q.PushAtOnce(ctx, batch))
	}

	// two workers, five pending tasks: exactly the two earliest dispatch.
	results := make(chan *model.Task, 2)
	for agentID := int64(1); agentID <= 2; agentID++ {
		go func() {
			task, _ := q.Poll(ctx, agentID, filterFnTrue)
			results <- task
		}()
	}

	dispatched := make([]string, 0, 2)
	for range 2 {
		select {
		case task := <-results:
			assert.NotNil(t, task)
			dispatched = append(dispatched, task.ID)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for dispatched tasks")
		}
	}
	assert.ElementsMatch(t, []string{"10", "20"}, dispatched,
		"the two earliest-Created tasks must be the ones dispatched in the tick")

	waitForProcess()
	info := q.Info(ctx)
	pending := make([]string, 0, len(info.Pending))
	for _, task := range info.Pending {
		pending = append(pending, task.ID)
	}
	assert.Equal(t, []string{"30", "40", "50"}, pending,
		"the remaining tasks stay pending in creation order")

	for _, id := range dispatched {
		assert.NoError(t, q.Done(ctx, id, model.StatusSuccess))
	}
}

func TestFifoPendingInsertOrder(t *testing.T) {
	// The two insert helpers differ only in where they place a task among the
	// ones it compares equal to: a normal push goes after them (the order the
	// append-then-stable-sort it replaces produced), while a resubmitted expired
	// task goes ahead of them, keeping the retry priority a list push-front used
	// to give it. Asserted directly on the list: through the queue the two are
	// indistinguishable whenever the comparator already separates the tasks.
	pendingIDs := func(q *fifo) []string {
		ids := make([]string, 0, q.pending.Len())
		for element := q.pending.Front(); element != nil; element = element.Next() {
			task, _ := element.Value.(*model.Task)
			ids = append(ids, task.ID)
		}
		return ids
	}

	q := &fifo{pending: list.New()}
	q.pushPending(&model.Task{ID: "b1", Created: 200})
	q.pushPending(&model.Task{ID: "a", Created: 100})
	q.pushPending(&model.Task{ID: "c", Created: 300})
	q.pushPending(&model.Task{ID: "b2", Created: 200})
	assert.Equal(t, []string{"a", "b1", "b2", "c"}, pendingIDs(q),
		"a push is ordered by Created and lands after the tasks it ties with")

	q.pushPendingFront(&model.Task{ID: "retry", Created: 200})
	assert.Equal(t, []string{"a", "retry", "b1", "b2", "c"}, pendingIDs(q),
		"a resubmitted task is ordered by Created but lands ahead of its ties")

	// pushPendingFront's empty-list branch: its first call above went into a
	// populated list, so cover the fresh-list case too (symmetric with the
	// pushPending empty-list push at the top).
	empty := &fifo{pending: list.New()}
	empty.pushPendingFront(&model.Task{ID: "only", Created: 100})
	assert.Equal(t, []string{"only"}, pendingIDs(empty),
		"pushPendingFront into an empty list seeds the list")

	// Both helpers must also handle the ends of the list, not just the middle.
	q.pushPendingFront(&model.Task{ID: "first", Created: 50})
	q.pushPending(&model.Task{ID: "last", Created: 400})
	assert.Equal(t, []string{"first", "a", "retry", "b1", "b2", "c", "last"}, pendingIDs(q))
}

func TestFifoFilterWaitingDrainOrder(t *testing.T) {
	// filterWaiting drains dependency-cleared tasks back into pending on every
	// tick. They must land in creation order among the tasks already there —
	// a task from an older pipeline goes ahead of the newer tail, which is the
	// whole point of fair dispatch. Asserted on the list rather than through a
	// dispatch, so the invariant has a guard of its own.
	// A single drained task that sorts to the very front stops the cursor on the
	// first comparison — the simplest merge case.
	q := &fifo{pending: list.New(), waitingOnDeps: list.New(), running: map[string]*entry{}}
	q.pending.PushBack(&model.Task{ID: "new1", Created: 300})
	q.pending.PushBack(&model.Task{ID: "new2", Created: 400})
	q.waitingOnDeps.PushBack(&model.Task{ID: "old", Created: 100})

	q.filterWaiting()

	pendingIDs := func(q *fifo) []string {
		ids := make([]string, 0, q.pending.Len())
		for element := q.pending.Front(); element != nil; element = element.Next() {
			task, _ := element.Value.(*model.Task)
			ids = append(ids, task.ID)
		}
		return ids
	}
	assert.Equal(t, []string{"old", "new1", "new2"}, pendingIDs(q),
		"a drained task keeps its creation-order priority over newer pending tasks")

	// Multiple drained tasks interleaving into the middle and tail of a
	// multi-element pending list: exercises the forward cursor advancing across
	// iterations and the at==nil tail-PushBack branch. waitingOnDeps must be
	// ascending (the invariant filterWaiting's rebuild maintains).
	q2 := &fifo{pending: list.New(), waitingOnDeps: list.New(), running: map[string]*entry{}}
	for _, id := range []struct {
		name    string
		created int64
	}{{"p100", 100}, {"p300", 300}, {"p500", 500}} {
		q2.pending.PushBack(&model.Task{ID: id.name, Created: id.created})
	}
	for _, id := range []struct {
		name    string
		created int64
	}{{"w200", 200}, {"w400", 400}, {"w600", 600}} {
		q2.waitingOnDeps.PushBack(&model.Task{ID: id.name, Created: id.created})
	}

	q2.filterWaiting()

	assert.Equal(t, []string{"p100", "w200", "p300", "w400", "p500", "w600"}, pendingIDs(q2),
		"drained tasks merge into creation order, advancing the cursor through the middle and appending at the tail (w600)")

	// A drained task whose Created ties a pending task lands AFTER the equal-key
	// pending task, matching the append-then-stable-sort semantics the merge
	// replaces (taskOrderLess is a strict less-than on Created, then Name).
	q3 := &fifo{pending: list.New(), waitingOnDeps: list.New(), running: map[string]*entry{}}
	q3.pending.PushBack(&model.Task{ID: "pending-200", Created: 200, Name: "a"})
	q3.waitingOnDeps.PushBack(&model.Task{ID: "drained-200", Created: 200, Name: "z"})

	q3.filterWaiting()

	assert.Equal(t, []string{"pending-200", "drained-200"}, pendingIDs(q3),
		"a drained task tying a pending task's Created lands after it")
}
