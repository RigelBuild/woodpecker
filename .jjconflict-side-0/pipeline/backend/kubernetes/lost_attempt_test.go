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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kube_core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	kube_meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

const lostMarker = "woodpecker-ci.org/lost-attempt"

func requeuedWorkflow(t *testing.T) (*fake.Clientset, *kube, *types.Config, *types.Step) {
	t.Helper()
	client := fake.NewClientset()
	engine := makeEngine(client)
	engine.config.VolumeSize = "1Gi"
	step := makeStep("requeue-01")
	step.Environment = map[string]string{"CI_NETRC_PASSWORD": "clone-token"}
	conf := &types.Config{Volume: "wp_task-9_default", Stages: []*types.Stage{{Steps: []*types.Step{step}}}}

	// What the lost attempt left: workflow PVC and service, a step pod and its Secret.
	require.NoError(t, engine.SetupWorkflow(t.Context(), conf, "task-9"))
	require.NoError(t, startStepSecret(t.Context(), engine, step))
	pod := createPod(t, client, step, "test-ns")
	pvcs := client.CoreV1().PersistentVolumeClaims("test-ns")
	pvc, err := pvcs.Get(t.Context(), "wp-task-9-default", kube_meta_v1.GetOptions{})
	require.NoError(t, err)
	pvc.Annotations = map[string]string{lostMarker: "true"}
	_, err = pvcs.Update(t.Context(), pvc, kube_meta_v1.UpdateOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, pod)
	// The deployed agent Role grants create and delete on PVCs, but not get.
	client.PrependReactor("get", "persistentvolumeclaims", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, nil, errors.NewForbidden(kube_core_v1.Resource("persistentvolumeclaims"), "", nil)
	})
	return client, engine, conf, step
}

func claimAnnotations(t *testing.T, client *fake.Clientset) map[string]string {
	t.Helper()
	obj, err := client.Tracker().Get(kube_core_v1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "test-ns", "wp-task-9-default")
	require.NoError(t, err)
	pvc, ok := obj.(*kube_core_v1.PersistentVolumeClaim)
	require.True(t, ok)
	return pvc.Annotations
}

func TestSetupWorkflowClearsRequeuedAttempt(t *testing.T) {
	client, engine, conf, step := requeuedWorkflow(t)

	require.NoError(t, engine.SetupWorkflow(t.Context(), conf, "task-9"))

	assert.NotContains(t, claimAnnotations(t, client), lostMarker, "the lost attempt's workspace must not be reused")
	// StartStep must be able to create the step's pod and Secret again.
	pod, err := podName(step)
	require.NoError(t, err)
	_, err = client.CoreV1().Pods("test-ns").Get(t.Context(), pod, kube_meta_v1.GetOptions{})
	assert.True(t, errors.IsNotFound(err), "leftover step pod should be deleted, got %v", err)
	secretName, err := stepSecretName(step)
	require.NoError(t, err)
	_, err = client.CoreV1().Secrets("test-ns").Get(t.Context(), secretName, kube_meta_v1.GetOptions{})
	assert.True(t, errors.IsNotFound(err), "leftover step secret should be deleted, got %v", err)
}

// The PVC protection finalizer keeps a deleted claim, so its create still conflicts.
func TestSetupWorkflowWaitsForTerminatingClaim(t *testing.T) {
	client, engine, conf, _ := requeuedWorkflow(t)
	lostAttemptPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { lostAttemptPollInterval = time.Second })
	stillTerminating := 3
	client.PrependReactor("create", "persistentvolumeclaims", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		if stillTerminating == 0 {
			return false, nil, nil
		}
		stillTerminating--
		return true, nil, errors.NewAlreadyExists(kube_core_v1.Resource("persistentvolumeclaims"), "wp-task-9-default")
	})

	require.NoError(t, engine.SetupWorkflow(t.Context(), conf, "task-9"))
	assert.Zero(t, stillTerminating, "setup must wait until the leftover claim is gone")
}

// A pod in its grace period keeps its name, so StartStep would collide with it.
func TestSetupWorkflowWaitsForTerminatingPod(t *testing.T) {
	client, engine, conf, _ := requeuedWorkflow(t)
	lostAttemptPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { lostAttemptPollInterval = time.Second })
	stillTerminating := 3
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		if stillTerminating == 0 {
			return false, nil, nil
		}
		stillTerminating--
		return true, &kube_core_v1.Pod{}, nil
	})

	require.NoError(t, engine.SetupWorkflow(t.Context(), conf, "task-9"))
	assert.Zero(t, stillTerminating, "setup must wait until the leftover pod is gone")
}

func TestSetupWorkflowGivesUpOnStuckLeftover(t *testing.T) {
	client, engine, conf, _ := requeuedWorkflow(t)
	lostAttemptPollInterval = 10 * time.Millisecond
	engine.config.StopTimeout = -59 // poll budget is StopTimeout + 1m, so 1s here
	t.Cleanup(func() { lostAttemptPollInterval = time.Second })
	client.PrependReactor("create", "persistentvolumeclaims", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, nil, errors.NewAlreadyExists(kube_core_v1.Resource("persistentvolumeclaims"), "wp-task-9-default")
	})

	err := engine.SetupWorkflow(t.Context(), conf, "task-9")
	assert.ErrorContains(t, err, "earlier attempt")
}
