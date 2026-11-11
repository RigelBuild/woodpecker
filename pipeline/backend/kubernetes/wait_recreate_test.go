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
	kube_meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// recreateTestPod is a step pod in the given phase, with its one container in state.
func recreateTestPod(name string, phase kube_core_v1.PodPhase, state kube_core_v1.ContainerState) *kube_core_v1.Pod {
	return &kube_core_v1.Pod{
		ObjectMeta: kube_meta_v1.ObjectMeta{Name: name, Namespace: "test-ns"},
		Spec:       kube_core_v1.PodSpec{Containers: []kube_core_v1.Container{{Name: "step"}}},
		Status: kube_core_v1.PodStatus{
			Phase:             phase,
			ContainerStatuses: []kube_core_v1.ContainerStatus{{Name: "step", State: state}},
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

// watchPods signals once the informer behind WaitStep has registered its watch.
func watchPods(client *fake.Clientset) <-chan struct{} {
	watching := make(chan struct{})
	var once atomic.Bool
	client.PrependWatchReactor("pods", func(action kube_testing.Action) (bool, watch.Interface, error) {
		w, err := client.Tracker().Watch(action.GetResource(), action.GetNamespace())
		if once.CompareAndSwap(false, true) {
			close(watching)
		}
		return true, w, err
	})
	return watching
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
	<-watching
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

	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), recreateTestPod(podName, kube_core_v1.PodRunning, runningState()), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	markRunning(t, engine, step, podName)
	creates := countPodCreates(client)
	watching := watchPods(client)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := engine.WaitStep(ctx, step, "task-recreate-twice")
		done <- err
	}()
	<-watching
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))
	waitForRecreate(t, client, podName)
	// Delete the replacement too: the second loss must fail, not recreate again.
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))

	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, errors.Is(err, types.ErrStepPodLost), "got %v", err)
	case <-ctx.Done():
		t.Fatal("WaitStep did not fail after the second loss")
	}
	assert.Equal(t, int32(1), creates.Load(), "a second loss must not trigger another recreate")
}

// A pod whose container exited 0 is not recreated when WaitStep finds the object still present.
func TestWaitStepDoesNotRecreateFinishedPod(t *testing.T) {
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
