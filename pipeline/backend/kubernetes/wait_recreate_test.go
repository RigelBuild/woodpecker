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

package kubernetes

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kube_core_v1 "k8s.io/api/core/v1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	kube_meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// recreateTestPod is a step pod in the given phase, with its one container in state.
func recreateTestPod(name string, phase kube_core_v1.PodPhase, state kube_core_v1.ContainerState) *kube_core_v1.Pod {
	// The step container is named after the pod, as production does (see stepContainerStatus).
	return &kube_core_v1.Pod{
		ObjectMeta: kube_meta_v1.ObjectMeta{Name: name, Namespace: "test-ns"},
		Spec:       kube_core_v1.PodSpec{Containers: []kube_core_v1.Container{{Name: name}}},
		Status: kube_core_v1.PodStatus{
			Phase:             phase,
			ContainerStatuses: []kube_core_v1.ContainerStatus{{Name: name, State: state}},
		},
	}
}

func runningState() kube_core_v1.ContainerState {
	return kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}
}

func exitedState(code int32) kube_core_v1.ContainerState {
	return kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: code}}
}

// countPodCreates counts pod creates through the fake clientset and lets each one through.
func countPodCreates(client *fake.Clientset) *atomic.Int32 {
	var creates atomic.Int32
	client.PrependReactor("create", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		creates.Add(1)
		return false, nil, nil
	})
	return &creates
}

// watchPods signals once per watch the informers register, so a test can wait for the first or the
// second. It forwards the watch's ListOptions to the tracker, so a watch resumed from the list's
// resourceVersion replays any object stored after the list. Dropping them loses an Update that
// lands between List and Watch.
func watchPods(client *fake.Clientset) <-chan struct{} {
	registered := make(chan struct{}, 8)
	client.PrependWatchReactor("pods", func(action kube_testing.Action) (bool, watch.Interface, error) {
		w, err := client.Tracker().Watch(action.GetResource(), action.GetNamespace(), action.(kube_testing.WatchActionImpl).ListOptions)
		registered <- struct{}{}
		return true, w, err
	})
	return registered
}

// awaitRegistration waits for the nth watch registration, bounded, so a missing watch fails fast.
func awaitRegistration(t *testing.T, registered <-chan struct{}, n int) {
	t.Helper()
	for range n {
		select {
		case <-registered:
		case <-time.After(5 * time.Second):
			t.Fatalf("watch registration %d never happened", n)
		}
	}
}

// snapshotGate returns a channel closed once the informer stores a Running snapshot for podName.
// The initial list reaches the informer as a store Replace, not a watch event, so the signal comes
// from the production handler through snapshotObserved, which runs after the snapshot is stored.
func snapshotGate(engine *kube, podName string) <-chan struct{} {
	gate := make(chan struct{})
	var once atomic.Bool
	engine.snapshotObserved = func(name string) {
		if name == podName && once.CompareAndSwap(false, true) {
			close(gate)
		}
	}
	return gate
}

// awaitGate waits for a gate with a bound, so a broken gate fails the test instead of hanging.
func awaitGate(t *testing.T, gate <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-gate:
	case <-time.After(5 * time.Second):
		t.Fatalf("gate never fired: %s", what)
	}
}

// markRunning drives the production waitPodUp path, which records that the pod reached Running.
func markRunning(t *testing.T, engine *kube, step *types.Step, podName string) {
	t.Helper()
	gone, err := engine.waitPodUp(t.Context(), step, podName)
	require.NoError(t, err)
	require.False(t, gone)
}

// waitForRecreate waits until the pod object exists again after it was deleted.
func waitForRecreate(t *testing.T, client *fake.Clientset, podName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := client.CoreV1().Pods("test-ns").Get(t.Context(), podName, kube_meta_v1.GetOptions{})
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
}

// A pod deleted after it reached Running is recreated once; WaitStep reports the new pod's exit code.
func TestWaitStepRecreatesPodDeletedAfterRunning(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("recreate-once")
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), recreateTestPod(podName, kube_core_v1.PodRunning, runningState()), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	creates := countPodCreates(client)
	watching := watchPods(client)
	running := snapshotGate(engine, podName)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type result struct {
		state *types.State
		err   error
	}
	done := make(chan result, 1)
	go func() {
		state, err := engine.WaitStep(ctx, step, "task-recreate")
		done <- result{state, err}
	}()
	awaitGate(t, watching, "informer watch registered")
	// The Running snapshot is stored before the delete, so the loss has a snapshot to decide on.
	awaitGate(t, running, "Running snapshot stored by WaitStep's informer")
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))
	waitForRecreate(t, client, podName)
	// The replacement exits 7, a code only the new pod can report: the lost pod never exited.
	_, err = client.CoreV1().Pods("test-ns").Update(t.Context(), recreateTestPod(podName, kube_core_v1.PodSucceeded, exitedState(7)), kube_meta_v1.UpdateOptions{})
	require.NoError(t, err)

	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.NotNil(t, res.state)
		assert.Equal(t, 7, res.state.ExitCode)
		assert.True(t, res.state.Exited)
	case <-ctx.Done():
		t.Fatal("WaitStep did not return after the recreated pod finished")
	}
	assert.Equal(t, int32(1), creates.Load(), "the lost pod must be recreated exactly once")
}

// A second loss of the recreated pod fails WaitStep with the loss sentinel and no second recreate.
func TestWaitStepFailsWhenRecreatedPodIsDeletedAgain(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("recreate-twice")
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	pod := recreateTestPod(podName, kube_core_v1.PodRunning, runningState())
	pod.Spec.NodeName = "node-1"
	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), pod, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	createPreemptEvent(t, client, "node-1")
	creates := countPodCreates(client)
	watching := watchPods(client)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := engine.WaitStep(ctx, step, "task-recreate-twice")
		done <- err
	}()
	awaitRegistration(t, watching, 1)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))
	waitForRecreate(t, client, podName)
	// The second delete must land after the second attempt's watch is registered, so the loss is observed.
	awaitRegistration(t, watching, 1)
	// Assert the replacement is stored before the second delete, so the delete is observable.
	_, err = client.CoreV1().Pods("test-ns").Get(t.Context(), podName, kube_meta_v1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))

	select {
	case err := <-done:
		var loss *types.PodLossError
		require.ErrorAs(t, err, &loss)
		assert.True(t, errors.Is(err, types.ErrStepPodLost), "got %v", err)
		// The replacement was never scheduled, so the second loss must not reuse the first pod's node.
		assert.Empty(t, loss.Node)
		assert.False(t, loss.Confirmed)
	case <-ctx.Done():
		t.Fatal("WaitStep did not fail after the second loss")
	}
	assert.Equal(t, int32(1), creates.Load(), "a second loss must not trigger another recreate")
}

// A container that exited 0 while the pod object is still present is not recreated.
func TestWaitStepDoesNotRecreateExitedPodWhileObjectPresent(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("finished-deleted")
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), recreateTestPod(podName, kube_core_v1.PodRunning, runningState()), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	// The container exits 0 while the pod object still exists when WaitStep looks it up.
	_, err = client.CoreV1().Pods("test-ns").Update(t.Context(), recreateTestPod(podName, kube_core_v1.PodSucceeded, exitedState(0)), kube_meta_v1.UpdateOptions{})
	require.NoError(t, err)
	creates := countPodCreates(client)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	state, err := engine.WaitStep(ctx, step, "task-finished")
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, 0, state.ExitCode)
	assert.Equal(t, int32(0), creates.Load(), "a finished pod must not be recreated")
}

// A single loss with the started marker cleared reports the node waitPodUp recorded, confirmed by
// a PreemptScheduled event on that node, and is not recreated.
func TestWaitStepReportsSingleLossNodeWithoutRecreate(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("single-loss-node")
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	pod := recreateTestPod(podName, kube_core_v1.PodRunning, runningState())
	pod.Spec.NodeName = "node-1"
	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), pod, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	createPreemptEvent(t, client, "node-1")
	// Clear the started marker first: this loss must be reported as a plain single loss.
	engine.started.Delete(podName)
	creates := countPodCreates(client)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	state, err := engine.WaitStep(ctx, step, "task-single-loss")
	var loss *types.PodLossError
	require.ErrorAs(t, err, &loss)
	assert.Nil(t, state)
	assert.Equal(t, "node-1", loss.Node)
	assert.True(t, loss.Confirmed, "the PreemptScheduled event on node-1 confirms the loss")
	assert.Equal(t, int32(0), creates.Load(), "a loss without the started marker must not recreate")
}

// createPreemptEvent records a PreemptScheduled event for node, the evidence podLoss looks for.
func createPreemptEvent(t *testing.T, client *fake.Clientset, node string) {
	t.Helper()
	_, err := client.CoreV1().Events("default").Create(t.Context(), &kube_core_v1.Event{
		ObjectMeta:     kube_meta_v1.ObjectMeta{Name: "preempt-" + node, Namespace: "default"},
		InvolvedObject: kube_core_v1.ObjectReference{Kind: "Node", Name: node},
		Reason:         preemptEventReason,
		Source:         kube_core_v1.EventSource{Component: preemptEventSource},
		EventTime:      kube_meta_v1.NewMicroTime(time.Now()),
	}, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
}

// A pod whose Get is NotFound before any watch DELETED reaches the handler is not recreated.
// The reactor reports NotFound from the start, so the loss is decided on the bounded wait alone.
func TestWaitStepDoesNotRecreateWithoutObservedDelete(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("no-observed-delete")
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), recreateTestPod(podName, kube_core_v1.PodRunning, runningState()), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	creates := countPodCreates(client)
	// Every Get reports NotFound before WaitStep starts, and no watch DELETED is ever sent.
	client.PrependReactor("get", "pods", func(action kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, nil, kube_errors.NewNotFound(kube_core_v1.Resource("pods"), podName)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	state, err := engine.WaitStep(ctx, step, "task-no-observed")
	var loss *types.PodLossError
	require.ErrorAs(t, err, &loss)
	assert.Nil(t, state)
	assert.Equal(t, int32(0), creates.Load(), "a pod with no observed delete must not be recreated")
}

// A pod recorded as disrupted (DisruptionTarget=True) whose container then exited 137 and was
// deleted is not recreated. Only an abrupt node loss is recreated: the container must still be
// running or waiting at its last snapshot, and a Terminated container never is.
func TestWaitStepDoesNotRecreateDisruptionTargetTerminated(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("disruption-terminated")
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	// Start Running so waitPodUp sets the started marker; a pod created Failed would never set it.
	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), recreateTestPod(podName, kube_core_v1.PodRunning, runningState()), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	creates := countPodCreates(client)
	watching := watchPods(client)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := engine.WaitStep(ctx, step, "task-disruption")
		done <- err
	}()
	awaitGate(t, watching, "informer watch registered")
	// An abrupt node loss: the container exits 137 and the disruption condition is set, then the object goes.
	disrupted := recreateTestPod(podName, kube_core_v1.PodFailed, exitedState(137))
	disrupted.Status.Conditions = []kube_core_v1.PodCondition{{
		Type:   kube_core_v1.DisruptionTarget,
		Status: kube_core_v1.ConditionTrue,
		Reason: "DeletionByTaintManager",
	}}
	_, err = client.CoreV1().Pods("test-ns").Update(t.Context(), disrupted, kube_meta_v1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))

	select {
	case err := <-done:
		var loss *types.PodLossError
		require.ErrorAs(t, err, &loss)
		assert.Equal(t, int32(0), creates.Load(), "a disrupted pod whose container exited must not be recreated")
	case <-ctx.Done():
		t.Fatal("WaitStep did not return after the delete")
	}
}

// deletedLoss decides the recreate from the observed-delete fact, the last snapshot, and the
// container state. Each row pins one branch of the rule with no informer timing.
func TestDeletedLoss(t *testing.T) {
	const podName = "wp-step"
	lossErr := errors.New("lost")
	withContainer := func(state kube_core_v1.ContainerState) *kube_core_v1.Pod {
		return &kube_core_v1.Pod{Status: kube_core_v1.PodStatus{
			ContainerStatuses: []kube_core_v1.ContainerStatus{{Name: podName, State: state}},
		}}
	}
	cases := []struct {
		name     string
		last     *kube_core_v1.Pod
		observed bool
		recreate bool
	}{
		{"terminated and observed does not recreate", withContainer(exitedState(1)), true, false},
		{"running and observed recreates", withContainer(runningState()), true, true},
		{"waiting and observed recreates", withContainer(kube_core_v1.ContainerState{Waiting: &kube_core_v1.ContainerStateWaiting{}}), true, true},
		{"running and not observed does not recreate", withContainer(runningState()), false, false},
		{"nil snapshot does not recreate", nil, true, false},
		{"container missing does not recreate", &kube_core_v1.Pod{}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deletedLoss(tc.last, tc.observed, podName, lossErr)
			if tc.recreate {
				var deleted *deletedPodError
				require.ErrorAs(t, got, &deleted)
				assert.ErrorIs(t, got, lossErr)
				return
			}
			assert.Same(t, lossErr, got, "no recreate: the original loss is returned unchanged")
		})
	}
}

// A pod lost before it reached Running is reported, not recreated.
// This is the boundary the RIG-9399 ruling keeps: Pending losses stay uncovered.
func TestWaitStepDoesNotRecreatePendingLoss(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("pending-no-recreate")
	podName := createPod(t, client, step, "test-ns")
	creates := countPodCreates(client)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	state, err := engine.WaitStep(ctx, step, "task-pending")
	var loss *types.PodLossError
	require.ErrorAs(t, err, &loss)
	assert.Nil(t, state)
	assert.Equal(t, int32(0), creates.Load(), "a Pending loss must not be recreated")
}

// The node is recorded from the initial add of a Running pod. The first loss recreates it, and the
// second loss of the unscheduled replacement reports no node.
func TestWaitPodUpRecordsNodeFromInitialAdd(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("pod-running-initial")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	pod := recreateTestPod(podName, kube_core_v1.PodRunning, runningState())
	pod.Spec.NodeName = "node-1"
	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), pod, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	createPreemptEvent(t, client, "node-1")

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	gone, err := engine.waitPodUp(ctx, step, podName)
	require.NoError(t, err)
	require.False(t, gone)
	value, ok := engine.nodes.Load(podName)
	require.True(t, ok)
	assert.Equal(t, "node-1", value)

	// WaitStep's informer must observe the Running pod before it is deleted, so the loss has a snapshot.
	watching := watchPods(client)
	creates := countPodCreates(client)
	var teardown atomic.Bool
	ctx, cancel = context.WithTimeout(types.WithTeardown(t.Context(), &teardown), 5*time.Second)
	defer cancel()
	type result struct {
		state *types.State
		err   error
	}
	done := make(chan result, 1)
	go func() {
		state, err := engine.WaitStep(ctx, step, "task-1")
		done <- result{state, err}
	}()
	awaitRegistration(t, watching, 1)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))
	waitForRecreate(t, client, podName)
	// The second delete must land after the second attempt's watch is registered, so the loss is observed.
	awaitRegistration(t, watching, 1)
	// Assert the replacement is stored before the second delete, so the delete is observable.
	_, err = client.CoreV1().Pods("test-ns").Get(t.Context(), podName, kube_meta_v1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))

	select {
	case res := <-done:
		var loss *types.PodLossError
		require.ErrorAs(t, res.err, &loss)
		assert.Nil(t, res.state)
		assert.Equal(t, int32(1), creates.Load(), "the first loss recreates the pod once; the second fails")
		assert.Empty(t, loss.Node, "the replacement was never scheduled")
		assert.False(t, loss.Confirmed)
	case <-ctx.Done():
		t.Fatal("WaitStep did not fail after the second loss")
	}
}
