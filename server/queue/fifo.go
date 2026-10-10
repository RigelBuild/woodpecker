// Copyright 2022 Woodpecker Authors
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

package queue

import (
	"container/list"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

type entry struct {
	item     *model.Task
	done     chan bool
	error    error
	deadline time.Time
}

type worker struct {
	agentID int64
	filter  func(*model.Task) (bool, int)
	channel chan *model.Task
	stop    context.CancelCauseFunc
}

type fifo struct {
	sync.Mutex

	ctx           context.Context
	workers       map[*worker]struct{}
	running       map[string]*entry
	pending       *list.List
	waitingOnDeps *list.List
	extension     time.Duration
	paused        bool
}

// processTimeInterval is the time till the queue rearranges things,
// as the agent pull in 10 milliseconds we should also give them work asap.
const processTimeInterval = 100 * time.Millisecond

// NewMemoryQueue returns a new fifo queue.
func NewMemoryQueue(ctx context.Context) Queue {
	q := &fifo{
		ctx:           ctx,
		workers:       map[*worker]struct{}{},
		running:       map[string]*entry{},
		pending:       list.New(),
		waitingOnDeps: list.New(),
		extension:     constant.TaskTimeout,
		paused:        false,
	}
	go q.process()
	return q
}

// PushAtOnce pushes multiple tasks to this queue, each into its creation-order
// position among the tasks already pending.
func (q *fifo) PushAtOnce(_ context.Context, tasks []*model.Task) error {
	q.Lock()
	for _, task := range tasks {
		q.pushPending(task)
	}
	q.Unlock()
	return nil
}

// Poll retrieves and removes a task head of this queue.
func (q *fifo) Poll(c context.Context, agentID int64, filter func(*model.Task) (bool, int)) (*model.Task, error) {
	q.Lock()
	ctx, stop := context.WithCancelCause(c)

	w := &worker{
		agentID: agentID,
		channel: make(chan *model.Task, 1),
		filter:  filter,
		stop:    stop,
	}
	q.workers[w] = struct{}{}
	q.Unlock()

	for {
		select {
		case <-ctx.Done():
			q.Lock()
			delete(q.workers, w)
			q.Unlock()
			return nil, ctx.Err()
		case t := <-w.channel:
			return t, nil
		}
	}
}

// Done signals the task is complete.
func (q *fifo) Done(_ context.Context, id string, exitStatus model.StatusValue) error {
	return q.finished([]string{id}, exitStatus, nil)
}

// Error signals the task is done with an error.
func (q *fifo) Error(_ context.Context, id string, err error) error {
	return q.finished([]string{id}, model.StatusFailure, err)
}

// ErrorAtOnce signals multiple tasks are done and complete with an error.
// If still pending they will just get removed from the queue.
func (q *fifo) ErrorAtOnce(_ context.Context, ids []string, err error) error {
	if errors.Is(err, ErrCancel) {
		return q.finished(ids, model.StatusKilled, err)
	}
	return q.finished(ids, model.StatusFailure, err)
}

// locks the queue itself!
func (q *fifo) finished(ids []string, exitStatus model.StatusValue, err error) error {
	q.Lock()
	defer q.Unlock()

	// it's an external error so we wrap it
	err = NewErrExternal(err)

	var errs []error
	// we first process the tasks itself
	for _, id := range ids {
		if taskEntry, ok := q.running[id]; ok {
			taskEntry.error = err
			close(taskEntry.done)
			delete(q.running, id)
		} else {
			errs = append(errs, q.removeFromPendingAndWaiting(id))
		}
	}

	// next we aim for there dependencies
	// we do this because in our ids list there could be tasks and its dependencies
	// so not to mess things up
	for _, id := range ids {
		q.updateDepStatusInQueue(id, exitStatus)
	}

	return errors.Join(errs...)
}

// Wait waits until the item is done executing.
// Also signals via error ErrCancel if workflow got canceled.
func (q *fifo) Wait(ctx context.Context, taskID string) error {
	q.Lock()
	state := q.running[taskID]
	q.Unlock()
	if state != nil {
		select {
		case <-ctx.Done():
		case <-state.done:
			// check if we have a wrapped cancel error and unwrap it
			if errors.Is(state.error, ErrCancel) {
				return ErrCancel
			}
			// or return queue errors and no workflow errors
			if !errors.Is(state.error, new(ErrExternal)) {
				return state.error
			}
		}
	}
	return nil
}

// Extend extends the task execution deadline.
func (q *fifo) Extend(_ context.Context, agentID int64, taskID string) error {
	q.Lock()
	defer q.Unlock()

	state, ok := q.running[taskID]
	if ok {
		if state.item.AgentID != agentID {
			return ErrAgentMissMatch
		}

		state.deadline = time.Now().Add(q.extension)
		return nil
	}
	return ErrNotFound
}

// Info returns internal queue information.
func (q *fifo) Info(_ context.Context) InfoT {
	q.Lock()
	stats := InfoT{}
	stats.Stats.Workers = len(q.workers)
	stats.Stats.Pending = q.pending.Len()
	stats.Stats.WaitingOnDeps = q.waitingOnDeps.Len()
	stats.Stats.Running = len(q.running)

	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		stats.Pending = append(stats.Pending, task)
	}
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		stats.WaitingOnDeps = append(stats.WaitingOnDeps, task)
	}
	for _, entry := range q.running {
		stats.Running = append(stats.Running, entry.item)
	}
	stats.Paused = q.paused

	q.Unlock()
	return stats
}

// Pause stops the queue from handing out new work items in Poll.
func (q *fifo) Pause() {
	q.Lock()
	q.paused = true
	q.Unlock()
}

// Resume starts the queue again.
func (q *fifo) Resume() {
	q.Lock()
	q.paused = false
	q.Unlock()
}

// KickAgentWorkers kicks all workers for a given agent.
func (q *fifo) KickAgentWorkers(agentID int64) {
	q.Lock()
	defer q.Unlock()

	for worker := range q.workers {
		if worker.agentID == agentID {
			worker.stop(ErrWorkerKicked)
			delete(q.workers, worker)
		}
	}
}

// helper function that loops through the queue and attempts to
// match the item to a single subscriber until context got cancel.
func (q *fifo) process() {
	for {
		select {
		case <-time.After(processTimeInterval):
		case <-q.ctx.Done():
			return
		}

		q.Lock()
		if q.paused {
			q.Unlock()
			continue
		}

		q.resubmitExpiredPipelines()
		q.filterWaiting()
		for pending, worker := q.assignToWorker(); pending != nil && worker != nil; pending, worker = q.assignToWorker() {
			task, _ := pending.Value.(*model.Task)
			task.AgentID = worker.agentID
			delete(q.workers, worker)
			q.pending.Remove(pending)
			q.running[task.ID] = &entry{
				item:     task,
				done:     make(chan bool),
				deadline: time.Now().Add(q.extension),
			}
			worker.channel <- task
		}
		q.Unlock()
	}
}

func (q *fifo) filterWaiting() {
	// Resubmit all waiting tasks to pending; deps may have cleared. Both lists
	// are ordered by taskOrderLess — waitingOnDeps is rebuilt below by scanning
	// pending in order — so this is a merge of two sorted lists, walked once,
	// rather than a search per task. A drained task from an older pipeline
	// therefore costs no more than the newer tail it sorts ahead of.
	at := q.pending.Front()
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		for at != nil {
			other, _ := at.Value.(*model.Task)
			if taskOrderLess(task, other) {
				break
			}
			at = at.Next()
		}
		if at == nil {
			q.pending.PushBack(task)
			continue
		}
		q.pending.InsertBefore(task, at)
	}

	// rebuild waitingDeps
	q.waitingOnDeps = list.New()
	var filtered []*list.Element
	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		if q.depsInQueue(task) {
			log.Debug().Msgf("queue: waiting due to unmet dependencies %v", task.ID)
			q.waitingOnDeps.PushBack(task)
			filtered = append(filtered, element)
		}
	}

	// filter waiting tasks
	for _, f := range filtered {
		q.pending.Remove(f)
	}
}

func (q *fifo) assignToWorker() (*list.Element, *worker) {
	var bestWorker *worker
	var bestScore int

	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		log.Debug().Msgf("queue: trying to assign task: %v with deps %v", task.ID, task.Dependencies)

		// skip tasks that would exceed their workflow concurrency limit, they
		// stay pending and are retried on the next process tick.
		if !q.canRunConcurrent(task) {
			log.Debug().Msgf("queue: task %v deferred due to concurrency group %q", task.ID, task.ConcurrencyGroup)
			continue
		}

		for worker := range q.workers {
			matched, score := worker.filter(task)
			if matched && score > bestScore {
				bestWorker = worker
				bestScore = score
			}
		}
		if bestWorker != nil {
			log.Debug().Msgf("queue: assigned task: %v with deps %v to worker with score %d", task.ID, task.Dependencies, bestScore)
			return element, bestWorker
		}
	}

	return nil, nil
}

// pushPending inserts a task into the pending list, keeping the list ordered by
// taskOrderLess (creation time, then workflow name). The sort key is immutable
// once a task is queued, so maintaining the order at insertion time is
// equivalent to — and replaces — re-sorting the whole pending set on every
// dispatch. Dispatch therefore walks the list directly, and a workflow whose
// dependencies have cleared keeps its creation-order priority instead of being
// overtaken by tasks from pipelines created later.
//
// A task is inserted after the tasks it compares equal to, matching the
// append-then-stable-sort order this replaces. Expects the queue to be locked
// by the caller.
//
// The scan runs back-to-front, which is one step for the common case of a new
// pipeline's batch appended to the tail. A task belonging ahead of the whole
// list is that direction's worst case, so it is taken by a front check first.
// The check is on a strict less-than, so it never reorders equals.
func (q *fifo) pushPending(task *model.Task) {
	if front := q.pending.Front(); front == nil {
		q.pending.PushFront(task)
		return
	} else if other, _ := front.Value.(*model.Task); taskOrderLess(task, other) {
		q.pending.PushFront(task)
		return
	}

	for element := q.pending.Back(); element != nil; element = element.Prev() {
		other, _ := element.Value.(*model.Task)
		if !taskOrderLess(task, other) {
			q.pending.InsertAfter(task, element)
			return
		}
	}
	// Unreachable: the front-check above returns when the task sorts before the
	// head, so the back-to-front scan always finds an insertion point. Kept as a
	// defensive fallback.
	q.pending.PushFront(task)
}

// pushPendingFront inserts a task ahead of the tasks it compares equal to,
// otherwise keeping the taskOrderLess order pushPending maintains. A resubmitted
// expired task has already been dispatched once, so it keeps the head position
// among its equals that a plain list push-front used to give it.
// Expects the queue to be locked by the caller.
//
// The scan runs front-to-back, the good end for a resubmitted task from an
// older pipeline. A task sorting past the whole list is taken by a back check
// first, on a strict less-than so equals are never reordered.
func (q *fifo) pushPendingFront(task *model.Task) {
	if back := q.pending.Back(); back == nil {
		q.pending.PushBack(task)
		return
	} else if other, _ := back.Value.(*model.Task); taskOrderLess(other, task) {
		q.pending.PushBack(task)
		return
	}

	for element := q.pending.Front(); element != nil; element = element.Next() {
		other, _ := element.Value.(*model.Task)
		if !taskOrderLess(other, task) {
			q.pending.InsertBefore(task, element)
			return
		}
	}
	// Unreachable: the back-check above returns when the task sorts after the
	// tail, so the front-to-back scan always finds an insertion point. Kept as a
	// defensive fallback.
	q.pending.PushBack(task)
}

// canRunConcurrent reports whether the given task may currently start without
// violating its workflow concurrency limit. Tasks without a limit always pass,
// keeping the default scheduling behavior unchanged.
//
// Slots within a concurrency group are granted in creation order (earliest
// pipeline first, by the task's Created timestamp, with the workflow name as a
// deterministic tiebreaker) rather than in the order tasks become ready. This
// guarantees that a later pipeline whose dependencies happen to finish faster
// cannot overtake an earlier one that is still waiting.
//
// The ordering reservation only applies across pipelines. Within a single
// pipeline, execution order is already defined by depends_on, and reserving a
// slot for an earlier workflow could deadlock when it depends on a later
// workflow that shares the same group (e.g. deploy.yaml depending on
// test.yaml). Because dependencies never cross pipelines, restricting the
// reservation to other pipelines keeps the ordering guarantee while making
// such deadlocks impossible.
//
// Expects the queue to be locked by the caller.
func (q *fifo) canRunConcurrent(task *model.Task) bool {
	if task.ConcurrencyLimit <= 0 || task.ConcurrencyGroup == "" {
		return true
	}

	group := task.ConcurrencyGroup

	// count tasks of the same group that already occupy a running slot.
	running := 0
	for _, e := range q.running {
		if e.item.ConcurrencyGroup == group {
			running++
		}
	}
	if running >= task.ConcurrencyLimit {
		return false
	}

	// count not-yet-running members of the group from earlier pipelines. They
	// have priority for the remaining slots, even if they are still waiting on
	// their dependencies, so cross-pipeline ordering is preserved.
	ahead := 0
	countAhead := func(other *model.Task) {
		if other.ConcurrencyGroup != group || other.ID == task.ID {
			return
		}
		// only reserve order across pipelines; see the function doc above.
		if other.PipelineID == task.PipelineID {
			return
		}
		if taskOrderLess(other, task) {
			ahead++
		}
	}
	for element := q.pending.Front(); element != nil; element = element.Next() {
		other, _ := element.Value.(*model.Task)
		countAhead(other)
	}
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		other, _ := element.Value.(*model.Task)
		countAhead(other)
	}

	return running+ahead < task.ConcurrencyLimit
}

// taskOrderLess reports whether task a was instantiated before task b. Ordering
// is by the Created timestamp (the pipeline creation time), with the workflow
// name as a deterministic tiebreaker for tasks created within the same second.
// The task ID is intentionally not used for ordering.
func taskOrderLess(a, b *model.Task) bool {
	if a.Created != b.Created {
		return a.Created < b.Created
	}
	return a.Name < b.Name
}

func (q *fifo) resubmitExpiredPipelines() {
	for taskID, taskState := range q.running {
		if time.Now().After(taskState.deadline) {
			log.Info().Msgf("queue: resubmitting expired task %s", taskID)
			taskState.error = ErrTaskExpired
			q.pushPendingFront(taskState.item)
			delete(q.running, taskID)
			close(taskState.done)
		}
	}
}

func (q *fifo) depsInQueue(task *model.Task) bool {
	for element := q.pending.Front(); element != nil; element = element.Next() {
		possibleDep, ok := element.Value.(*model.Task)
		log.Debug().Msgf("queue: pending right now: %v", possibleDep.ID)
		for _, dep := range task.Dependencies {
			if ok && possibleDep.ID == dep {
				return true
			}
		}
	}
	for possibleDepID := range q.running {
		log.Debug().Msgf("queue: running right now: %v", possibleDepID)
		if slices.Contains(task.Dependencies, possibleDepID) {
			return true
		}
	}
	return false
}

// expects the q to be currently owned e.g. locked by caller!
func (q *fifo) updateDepStatusInQueue(taskID string, status model.StatusValue) {
	for element := q.pending.Front(); element != nil; element = element.Next() {
		pending, _ := element.Value.(*model.Task)
		for _, dep := range pending.Dependencies {
			if taskID == dep {
				pending.DepStatus[dep] = status
			}
		}
	}

	for _, running := range q.running {
		for _, dep := range running.item.Dependencies {
			if taskID == dep {
				running.item.DepStatus[dep] = status
			}
		}
	}

	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		waiting, _ := element.Value.(*model.Task)
		for _, dep := range waiting.Dependencies {
			if taskID == dep {
				waiting.DepStatus[dep] = status
			}
		}
	}
}

// expects the q to be currently owned e.g. locked by caller!
func (q *fifo) removeFromPendingAndWaiting(taskID string) error {
	log.Debug().Msgf("queue: trying to remove %s", taskID)

	// we assume pending first
	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		if task.ID == taskID {
			log.Debug().Msgf("queue: %s is removed from pending", taskID)
			_ = q.pending.Remove(element)
			return nil
		}
	}

	// well looks like it's waiting
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		if task.ID == taskID {
			log.Debug().Msgf("queue: %s is removed from waitingOnDeps", taskID)
			_ = q.waitingOnDeps.Remove(element)
			return nil
		}
	}

	// well it could not be found
	return ErrNotFound
}
