// Copyright 2024 Woodpecker Authors
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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	kube_core_v1 "k8s.io/api/core/v1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	kube_meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

func TestGettingConfig(t *testing.T) {
	engine := kube{
		config: &config{
			Namespace:            "default",
			StorageClass:         "hdd",
			VolumeSize:           "1G",
			StorageRwx:           false,
			PodLabels:            map[string]string{"l1": "v1"},
			PodAnnotations:       map[string]string{"a1": "v1"},
			ImagePullSecretNames: []string{"regcred"},
			SecurityContext:      SecurityContextConfig{RunAsNonRoot: false},
		},
	}
	config := engine.getConfig()
	config.Namespace = "wp"
	config.StorageClass = "ssd"
	config.StorageRwx = true
	config.PodLabels = nil
	config.PodAnnotations["a2"] = "v2"
	config.ImagePullSecretNames = append(config.ImagePullSecretNames, "docker.io")
	config.SecurityContext.RunAsNonRoot = true

	assert.Equal(t, "default", engine.config.Namespace)
	assert.Equal(t, "hdd", engine.config.StorageClass)
	assert.Equal(t, "1G", engine.config.VolumeSize)
	assert.False(t, engine.config.StorageRwx)
	assert.Len(t, engine.config.PodLabels, 1)
	assert.Len(t, engine.config.PodAnnotations, 1)
	assert.Len(t, engine.config.ImagePullSecretNames, 1)
	assert.False(t, engine.config.SecurityContext.RunAsNonRoot)
}

func TestSetupWorkflow(t *testing.T) {
	namespace := "foo"
	volumeName := "volume-name"
	volumePath := volumeName + ":/woodpecker"
	networkName := "test-network"
	taskUUID := "11301"

	engine := kube{
		config: &config{
			Namespace:            namespace,
			StorageClass:         "hdd",
			VolumeSize:           "1G",
			StorageRwx:           false,
			PodLabels:            map[string]string{"l1": "v1"},
			PodAnnotations:       map[string]string{"a1": "v1"},
			ImagePullSecretNames: []string{"regcred"},
			SecurityContext:      SecurityContextConfig{RunAsNonRoot: false},
		},
		client: fake.NewClientset(),
	}

	serviceWithPorts := types.Step{
		OrgID:    42,
		Name:     "service",
		UUID:     "123",
		Type:     types.StepTypeService,
		Volumes:  []string{volumePath},
		Networks: []types.Conn{{Name: networkName, Aliases: []string{"alias"}}},
		Ports: []types.Port{
			{Number: 8080, Protocol: "tcp"},
		},
	}

	conf := &types.Config{
		Volume:  volumePath,
		Network: networkName,
		Stages: []*types.Stage{
			{
				Steps: []*types.Step{
					&serviceWithPorts,
					{
						OrgID:    42,
						UUID:     "234",
						Name:     "service2",
						Type:     types.StepTypeService,
						Volumes:  []string{volumePath},
						Networks: []types.Conn{{Name: networkName, Aliases: []string{"alias"}}},
					},
				},
			},
			{
				Steps: []*types.Step{
					{
						OrgID:    42,
						UUID:     "456",
						Name:     "step-1",
						Volumes:  []string{volumePath},
						Networks: []types.Conn{{Name: networkName, Aliases: []string{"alias"}}},
					},
				},
			},
		},
	}

	err := engine.SetupWorkflow(t.Context(), conf, taskUUID)
	assert.NoError(t, err, "SetupWorkflow should not error with minimal config and fake client")

	_, err = engine.client.CoreV1().PersistentVolumeClaims(namespace).Get(t.Context(), "volume-name", kube_meta_v1.GetOptions{})
	assert.NoError(t, err, "persistent volume should be created during workflow setup")

	_, err = engine.client.CoreV1().Services(namespace).Get(t.Context(), "wp-hsvc-"+taskUUID, kube_meta_v1.GetOptions{})
	assert.NoError(t, err, "headless service should be created during workflow setup")
}

func TestAffinityFromCliContext(t *testing.T) {
	t.Setenv("WOODPECKER_BACKEND_K8S_NAMESPACE", "")
	t.Setenv("WOODPECKER_BACKEND_K8S_POD_AFFINITY", `{
		"podAffinity": {
			"requiredDuringSchedulingIgnoredDuringExecution": [
			{
				"labelSelector": {},
				"matchLabelKeys": [
				"woodpecker-ci.org/task-uuid"
				],
				"topologyKey": "kubernetes.io/hostname"
			}
			]
		}
		}`)
	t.Setenv("WOODPECKER_BACKEND_K8S_POD_AFFINITY_ALLOW_FROM_STEP", "false")

	cmd := &cli.Command{
		Flags: Flags,
		Action: func(ctx context.Context, c *cli.Command) error {
			ctx = context.WithValue(ctx, types.CliCommand, c)
			config, err := configFromCliContext(ctx)

			require.NoError(t, err)
			require.NotNil(t, config)
			assert.False(t, config.PodAffinityAllowFromStep)

			// Verify affinity was parsed
			require.NotNil(t, config.PodAffinity)
			require.NotNil(t, config.PodAffinity.PodAffinity)
			require.Len(t, config.PodAffinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)

			term := config.PodAffinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
			assert.Equal(t, "kubernetes.io/hostname", term.TopologyKey)
			assert.Equal(t, []string{"woodpecker-ci.org/task-uuid"}, term.MatchLabelKeys)

			return nil
		},
	}
	err := cmd.Run(t.Context(), []string{"test"})
	require.NoError(t, err)
}

func makeStep(uuid string) *types.Step {
	return &types.Step{
		UUID:  uuid,
		Name:  "step-" + uuid,
		OrgID: 1,
	}
}

func makeEngine(client *fake.Clientset) *kube {
	return &kube{
		client: client,
		config: &config{
			Namespace: "test-ns",
		},
	}
}

func createPod(
	t *testing.T,
	client *fake.Clientset,
	step *types.Step,
	namespace string,
) string {
	t.Helper()
	podName, err := stepToPodName(step)
	require.NoError(t, err)

	pod := &kube_core_v1.Pod{
		Name:      podName,
		Namespace: namespace,
		Status: kube_core_v1.PodStatus{
			Phase: kube_core_v1.PodPending,
		},
	}
	_, err = client.CoreV1().Pods(namespace).Create(t.Context(), pod, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	return podName
}

func TestWaitStepReturnsOnContextCancel(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("ctx-cancel-01")
	namespace := "test-ns"

	createPod(t, client, step, namespace)

	ctx, cancel := context.WithCancelCause(t.Context())

	type result struct {
		state *types.State
		err   error
	}
	ch := make(chan result, 1)

	go func() {
		s, err := engine.WaitStep(ctx, step, "task-1")
		ch <- result{s, err}
	}()

	// Give the informer time to start and begin watching.
	time.Sleep(200 * time.Millisecond)

	cancel(nil)

	select {
	case r := <-ch:
		assert.Nil(t, r.state)
		assert.ErrorIs(t, r.err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("WaitStep did not return after context cancellation")
	}
}

func TestDestroyWorkflowDeletesStepSecretAfterFailedStart(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("netrc-only-03")
	step.Environment = map[string]string{"CI_NETRC_PASSWORD": "clone-token"}
	// StartStep's first half: the Secret exists, but the pod was never created.
	require.NoError(t, startStepSecret(t.Context(), engine, step))
	name, err := stepSecretName(step)
	require.NoError(t, err)

	err = engine.DestroyWorkflow(t.Context(), &types.Config{
		Volume: "task-3-workspace",
		Stages: []*types.Stage{{Steps: []*types.Step{step}}},
	}, "task-3")
	require.NoError(t, err)

	_, err = client.CoreV1().Secrets("test-ns").Get(t.Context(), name, kube_meta_v1.GetOptions{})
	assert.True(t, kube_errors.IsNotFound(err), "step secret should be deleted, got %v", err)
}

func TestWaitStepReturnsOnAlreadyDeletedPod(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("pod-delete-02")
	namespace := "test-ns"

	podName := createPod(t, client, step, namespace)

	var teardown atomic.Bool
	teardown.Store(true)
	teardownCtx := types.WithTeardown(t.Context(), &teardown)
	// Delete before WaitStep starts.
	err := client.CoreV1().Pods(namespace).Delete(teardownCtx, podName, kube_meta_v1.DeleteOptions{})
	require.NoError(t, err)

	type result struct {
		state *types.State
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := engine.WaitStep(teardownCtx, step, "task-1")
		ch <- result{s, err}
	}()

	select {
	case r := <-ch:
		require.NoError(t, r.err)
		require.NotNil(t, r.state)
		assert.True(t, r.state.Exited)
		assert.Equal(t, 0, r.state.ExitCode)
	case <-time.After(3 * time.Second):
		t.Fatal("WaitStep did not return for already-deleted pod")
	}
}

func TestWaitStepReturnsOnPodDeletedBeforeInformerSync(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("pod-delete-03")
	namespace := "test-ns"

	podName := createPod(t, client, step, namespace)

	// Delete the pod inside the informer's initial List, so it never enters the
	// cache and no delete event can arrive; only the guard Get can end the wait.
	deleted := false
	client.PrependReactor("list", "pods", func(action kube_testing.Action) (bool, kube_runtime.Object, error) {
		if deleted {
			return false, nil, nil
		}
		deleted = true
		if err := client.Tracker().Delete(action.GetResource(), namespace, podName); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})

	var teardown atomic.Bool
	teardown.Store(true)
	ctx, cancel := context.WithTimeout(types.WithTeardown(t.Context(), &teardown), 3*time.Second)
	defer cancel()

	state, err := engine.WaitStep(ctx, step, "task-1")
	require.NoError(t, err)
	require.True(t, deleted, "the informer's List should have removed the pod")
	require.NotNil(t, state)
	assert.True(t, state.Exited)
	assert.Equal(t, 0, state.ExitCode)
}

func TestWaitStepReturnsOnAlreadyTerminatedPod(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("pod-done-01")
	namespace := "test-ns"

	podName, err := stepToPodName(step)
	require.NoError(t, err)
	_, err = client.CoreV1().Pods(namespace).Create(t.Context(), &kube_core_v1.Pod{
		Name:      podName,
		Namespace: namespace,
		Status: kube_core_v1.PodStatus{
			Phase: kube_core_v1.PodFailed,
			ContainerStatuses: []kube_core_v1.ContainerStatus{{
				State: kube_core_v1.ContainerState{
					Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 1},
				},
			}},
		},
	}, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)

	// Below defaultResyncDuration, so a resync Update cannot end the wait instead.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	state, err := engine.WaitStep(ctx, step, "task-1")
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.True(t, state.Exited)
	assert.Equal(t, 1, state.ExitCode)
}

func TestWaitStepDetectsPodLoss(t *testing.T) {
	tests := []struct {
		name             string
		phase            kube_core_v1.PodPhase
		exitCode         int32
		condition        bool
		conditionReason  string
		deleteDuringList bool
		deleteAtGet      bool
		withNode         bool
		withEvent        bool
		eventAge         time.Duration
		wantLost         bool
		wantConfirmed    bool
		wantExitCode     int
		wantEvidence     string
		wantReason       string
	}{
		{name: "pod with DisruptionTarget and Preempt event deleted by guard Get", phase: kube_core_v1.PodFailed, exitCode: 143, condition: true, deleteAtGet: true, withNode: true, withEvent: true, wantLost: true, wantConfirmed: true, wantEvidence: preemptEventReason, wantReason: "DeletionByPodGC"},
		{name: "failed pod without evidence", phase: kube_core_v1.PodFailed, exitCode: 143, condition: true, conditionReason: "EvictionByEvictionAPI", withNode: true, wantLost: true, wantEvidence: "no matching PreemptScheduled event", wantReason: "EvictionByEvictionAPI"},
		{name: "disrupted pod after successful termination", phase: kube_core_v1.PodFailed, exitCode: 0, condition: true, wantExitCode: 0},
		{name: "pod deleted in initial informer list after tail recorded node", phase: kube_core_v1.PodRunning, deleteDuringList: true, withNode: true, withEvent: true, condition: true, wantLost: true, wantConfirmed: true, wantEvidence: preemptEventReason, wantReason: "NotFound"},
		{name: "succeeded pod", phase: kube_core_v1.PodSucceeded, exitCode: 0, wantExitCode: 0},
		{name: "old preempt event", phase: kube_core_v1.PodFailed, exitCode: 143, condition: true, deleteAtGet: true, withNode: true, withEvent: true, eventAge: 11 * time.Minute, wantLost: true, wantEvidence: "no matching PreemptScheduled event", wantReason: "DeletionByPodGC"},
		{name: "deleted pod with no node recorded", phase: kube_core_v1.PodRunning, deleteDuringList: true, wantLost: true, wantEvidence: "node unknown", wantReason: "NotFound"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewClientset()
			engine := makeEngine(client)
			step := makeStep("pod-loss-" + strings.ReplaceAll(test.name, " ", "-"))
			podName, err := stepToPodName(step)
			require.NoError(t, err)
			node := "node-1"
			if !test.withNode {
				node = ""
			}
			condition := []kube_core_v1.PodCondition(nil)
			if test.condition {
				reason := test.conditionReason
				if reason == "" {
					reason = "DeletionByPodGC"
				}
				condition = []kube_core_v1.PodCondition{{Type: kube_core_v1.DisruptionTarget, Status: kube_core_v1.ConditionTrue, Reason: reason}}
			}
			pod := &kube_core_v1.Pod{
				Name:      podName,
				Namespace: "test-ns",
				Spec:      kube_core_v1.PodSpec{NodeName: node},
				Status: kube_core_v1.PodStatus{
					Phase:             test.phase,
					Conditions:        condition,
					ContainerStatuses: []kube_core_v1.ContainerStatus{{Name: podName, State: kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: test.exitCode}}}},
				},
			}
			_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), pod, kube_meta_v1.CreateOptions{})
			require.NoError(t, err)
			if test.withNode {
				engine.recordNode(pod)
			}
			if test.withEvent {
				_, err = client.CoreV1().Events("default").Create(t.Context(), &kube_core_v1.Event{Name: "preempt", Namespace: "default", InvolvedObject: kube_core_v1.ObjectReference{Kind: "Node", Name: "node-1"}, Reason: preemptEventReason, Source: kube_core_v1.EventSource{Component: preemptEventSource}, EventTime: kube_meta_v1.NewMicroTime(time.Now().Add(-test.eventAge))}, kube_meta_v1.CreateOptions{})
				require.NoError(t, err)
			}
			if test.deleteAtGet {
				client.PrependReactor("get", "pods", func(action kube_testing.Action) (bool, kube_runtime.Object, error) {
					if err := client.Tracker().Delete(action.GetResource(), "test-ns", podName); err != nil {
						return true, nil, err
					}
					return false, nil, nil
				})
			}
			if test.deleteDuringList {
				client.PrependReactor("list", "pods", func(action kube_testing.Action) (bool, kube_runtime.Object, error) {
					if err := client.Tracker().Update(action.GetResource(), pod, "test-ns"); err != nil {
						return true, nil, err
					}
					if err := client.Tracker().Delete(action.GetResource(), "test-ns", podName); err != nil {
						return true, nil, err
					}
					return false, nil, nil
				})
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			state, err := engine.WaitStep(ctx, step, "task-1")
			if test.wantLost {
				var loss *types.PodLossError
				require.ErrorAs(t, err, &loss)
				assert.Equal(t, node, loss.Node)
				assert.Equal(t, test.wantReason, loss.Reason)
				assert.Equal(t, test.wantConfirmed, loss.Confirmed)
				assert.Equal(t, test.wantEvidence, loss.Evidence)
				assert.Nil(t, state)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, state)
			assert.Equal(t, test.wantExitCode, state.ExitCode)
		})
	}
}

func TestWaitPodUpRecordsNodeFromInitialAdd(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("pod-running-initial")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	pod := &kube_core_v1.Pod{
		Name:      podName,
		Namespace: "test-ns",
		Spec:      kube_core_v1.PodSpec{NodeName: "node-1"},
		Status:    kube_core_v1.PodStatus{Phase: kube_core_v1.PodRunning},
	}
	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), pod, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.NoError(t, engine.waitPodUp(ctx, step, podName))
	value, ok := engine.nodes.Load(podName)
	require.True(t, ok)
	assert.Equal(t, "node-1", value)

	_, err = client.CoreV1().Events("default").Create(t.Context(), &kube_core_v1.Event{
		Name:           "preempt",
		Namespace:      "default",
		InvolvedObject: kube_core_v1.ObjectReference{Kind: "Node", Name: "node-1"},
		Reason:         preemptEventReason, Source: kube_core_v1.EventSource{Component: preemptEventSource},
		EventTime: kube_meta_v1.NewMicroTime(time.Now()),
	}, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	deleted := false
	client.PrependReactor("list", "pods", func(action kube_testing.Action) (bool, kube_runtime.Object, error) {
		if deleted {
			return false, nil, nil
		}
		deleted = true
		if err := client.Tracker().Delete(action.GetResource(), "test-ns", podName); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	var teardown atomic.Bool
	ctx, cancel = context.WithTimeout(types.WithTeardown(t.Context(), &teardown), 3*time.Second)
	defer cancel()
	state, err := engine.WaitStep(ctx, step, "task-1")
	var loss *types.PodLossError
	require.ErrorAs(t, err, &loss)
	assert.Nil(t, state)
	assert.Equal(t, "node-1", loss.Node)
	assert.True(t, loss.Confirmed)
}

// TestDestroyWorkflowDeletesServicePodDuringTeardown keeps missing pods green during cleanup.
func TestDestroyWorkflowDeletesServicePodDuringTeardown(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("service-teardown")
	step.Type = types.StepTypeService
	step.OrgID = 1
	serviceName, err := stepToPodName(step)
	require.NoError(t, err)
	_, err = client.CoreV1().Pods("test-ns").Create(t.Context(), &kube_core_v1.Pod{
		Name:      serviceName,
		Namespace: "test-ns",
		Status:    kube_core_v1.PodStatus{Phase: kube_core_v1.PodRunning},
	}, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Services("test-ns").Create(t.Context(), &kube_core_v1.Service{Name: "wp-hsvc-task", Namespace: "test-ns"}, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	volumeName, err := volumeName("task")
	require.NoError(t, err)
	_, err = client.CoreV1().PersistentVolumeClaims("test-ns").Create(t.Context(), &kube_core_v1.PersistentVolumeClaim{Name: volumeName, Namespace: "test-ns"}, kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
	conf := &types.Config{Volume: "task", Stages: []*types.Stage{{Steps: []*types.Step{step}}}}
	var teardown atomic.Bool
	ctx := types.WithTeardown(t.Context(), &teardown)
	teardown.Store(true)
	require.NoError(t, engine.DestroyWorkflow(ctx, conf, "task"))
	state, err := engine.WaitStep(ctx, step, "task")
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, 0, state.ExitCode)
}

func TestWaitStepTreatsNotFoundAsGreenDuringTeardown(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("teardown-not-found")
	podName := createPod(t, client, step, "test-ns")
	require.NoError(t, client.CoreV1().Pods("test-ns").Delete(t.Context(), podName, kube_meta_v1.DeleteOptions{}))
	var teardown atomic.Bool
	teardown.Store(true)
	ctx, cancel := context.WithTimeout(types.WithTeardown(t.Context(), &teardown), 3*time.Second)
	defer cancel()
	state, err := engine.WaitStep(ctx, step, "task-1")
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, 0, state.ExitCode)
}

func TestWaitStepNoGoroutineLeak(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	namespace := "test-ns"
	numSteps := 10

	steps := make([]*types.Step, numSteps)
	for i := range numSteps {
		steps[i] = makeStep(fmt.Sprintf("leak-%02d", i))
		createPod(t, client, steps[i], namespace)
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := range numSteps {
		wg.Go(func() {
			ctx, cancel := context.WithCancelCause(t.Context())

			go func() {
				_, _ = engine.WaitStep(ctx, steps[i], fmt.Sprintf("task-%d", i))
			}()

			time.Sleep(200 * time.Millisecond)
			cancel(nil)
		})
	}
	wg.Wait()

	time.Sleep(1 * time.Second)

	afterCancelGoroutines := runtime.NumGoroutine()
	leaked := afterCancelGoroutines - baselineGoroutines

	assert.Less(t, leaked, numSteps,
		"goroutines leaked after canceling %d WaitStep calls: got %d leaked",
		numSteps, leaked)
}

func TestStartPodRetriesResourceQuotaConflict(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("quota-409-01")
	creates := 0
	client.PrependReactor("create", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		creates++
		if creates == 1 {
			return true, nil, quotaConflict()
		}
		return false, nil, nil
	})

	pod, err := startPod(t.Context(), engine, step, BackendOptions{}, "task-409")
	require.NoError(t, err)
	assert.Equal(t, 2, creates)
	_, err = client.CoreV1().Pods("test-ns").Get(t.Context(), pod.Name, kube_meta_v1.GetOptions{})
	require.NoError(t, err)
}

func quotaConflict() error {
	return kube_errors.NewConflict(
		kube_core_v1.Resource("resourcequotas"), "spend-cap", errors.New("the object has been modified"),
	)
}

func TestStartPodFailsAfterPersistentConflict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		engine := makeEngine(client)
		creates := 0
		client.PrependReactor("create", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
			creates++
			return true, nil, quotaConflict()
		})

		_, err := startPod(t.Context(), engine, makeStep("quota-409-02"), BackendOptions{}, "task-409")
		assert.True(t, kube_errors.IsConflict(err), "want conflict, got %v", err)
		assert.Equal(t, quotaConflictBackoff.Steps, creates)
	})
}

func TestStartPodStopsRetryingWhenCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		engine := makeEngine(client)
		ctx, cancel := context.WithCancelCause(t.Context())
		creates := 0
		client.PrependReactor("create", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
			creates++
			cancel(nil)
			return true, nil, quotaConflict()
		})

		_, err := startPod(ctx, engine, makeStep("quota-409-04"), BackendOptions{}, "task-409")
		assert.True(t, kube_errors.IsConflict(err), "want conflict, got %v", err)
		assert.Equal(t, 1, creates)
	})
}

func TestStartSecretsRetryResourceQuotaConflict(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	creates := 0
	client.PrependReactor("create", "secrets", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		creates++
		if creates%2 == 1 {
			return true, nil, quotaConflict()
		}
		return false, nil, nil
	})
	step := makeStep("quota-409-05")
	step.Image = "ghcr.io/example/image:1"
	step.AuthConfig.Username, step.AuthConfig.Password = "user", "pass"
	step.SecretMapping = map[string]string{"TOKEN": "value"}

	require.NoError(t, startRegistrySecret(t.Context(), engine, step))
	require.NoError(t, startStepSecret(t.Context(), engine, step))
	assert.Equal(t, 4, creates)
	secrets, err := client.CoreV1().Secrets("test-ns").List(t.Context(), kube_meta_v1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, secrets.Items, 2)
}

func TestSetupWorkflowRetriesResourceQuotaConflict(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	engine.config.VolumeSize = "1Gi"
	creates := map[string]int{}
	for _, resource := range []string{"persistentvolumeclaims", "services"} {
		client.PrependReactor("create", resource, func(kube_testing.Action) (bool, kube_runtime.Object, error) {
			creates[resource]++
			if creates[resource] == 1 {
				return true, nil, quotaConflict()
			}
			return false, nil, nil
		})
	}
	conf := &types.Config{
		Volume: "wp-vol:/woodpecker",
		Stages: []*types.Stage{{Steps: []*types.Step{makeStep("quota-409-03")}}},
	}

	require.NoError(t, engine.SetupWorkflow(t.Context(), conf, "task-409"))
	assert.Equal(t, map[string]int{"persistentvolumeclaims": 2, "services": 2}, creates)
	_, err := client.CoreV1().PersistentVolumeClaims("test-ns").Get(t.Context(), "wp-vol", kube_meta_v1.GetOptions{})
	require.NoError(t, err)
}

func TestStartPodDoesNotRetryOtherErrors(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("quota-403-01")
	creates := 0
	client.PrependReactor("create", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		creates++
		return true, nil, kube_errors.NewForbidden(kube_core_v1.Resource("pods"), "x", fmt.Errorf("exceeded quota"))
	})

	_, err := startPod(t.Context(), engine, step, BackendOptions{}, "task-403")
	assert.True(t, kube_errors.IsForbidden(err))
	assert.Equal(t, 1, creates)
}
