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
	"time"

	"github.com/rs/zerolog/log"
	kube_core_v1 "k8s.io/api/core/v1"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// deletedPodError marks a deleted Pending pod whose step container never started.
type deletedPodError struct {
	err error
}

func (e *deletedPodError) Error() string { return e.err.Error() }
func (e *deletedPodError) Unwrap() error { return e.err }

// deletedLoss permits recreation only for an observed delete of a Pending pod whose container never started.
func deletedLoss(last *kube_core_v1.Pod, observedDelete bool, podName string, err error) error {
	if !isRecreatableDeletedLoss(last, observedDelete, podName) {
		return err
	}
	return &deletedPodError{err: err}
}

func stepContainerStarted(container kube_core_v1.ContainerStatus, found bool) bool {
	return found && (container.State.Running != nil || container.State.Terminated != nil ||
		container.LastTerminationState.Terminated != nil || container.RestartCount > 0 ||
		(container.Started != nil && *container.Started))
}

func isRecreatableDeletedLoss(last *kube_core_v1.Pod, observedDelete bool, podName string) bool {
	if !observedDelete || last == nil || last.Status.Phase != kube_core_v1.PodPending {
		return false
	}
	container, found := stepContainerStatus(last, podName)
	return !stepContainerStarted(container, found)
}

func (e *kube) willRecreateDeletedPod(ctx context.Context, last *kube_core_v1.Pod, observedDelete bool, podName string) bool {
	if types.InTeardown(ctx) {
		return false
	}
	if _, started := e.started.Load(podName); started {
		return false
	}
	if isRecreatableDeletedLoss(last, observedDelete, podName) {
		return true
	}
	pending, found := e.pendingDeletes.Load(podName)
	pod, ok := pending.(*kube_core_v1.Pod)
	return found && ok && isRecreatableDeletedLoss(pod, true, podName)
}

// awaitObservedDelete waits for a direct watch delete; other disappearance signals fail closed.
func awaitObservedDelete(ctx context.Context, deleted <-chan struct{}) bool {
	timer := time.NewTimer(defaultResyncDuration)
	defer timer.Stop()
	select {
	case <-deleted:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// stepBackendOptions resolves the backend options a step pod is created with.
// Clone steps without explicit resources take the workflow's reserved resources.
// StartStep and the WaitStep recreate both use it, so a recreated pod matches the original.
func (e *kube) stepBackendOptions(step *types.Step, taskUUID string) BackendOptions {
	options, err := parseBackendOptions(step)
	if err != nil {
		log.Error().Err(err).Msg("could not parse backend options")
	}
	if step.Type == types.StepTypeClone && len(options.Resources.Requests) == 0 && len(options.Resources.Limits) == 0 {
		options.Resources = e.getWorkflowResources(taskUUID)
	}
	return options
}

// WaitStep recreates a deleted Pending pod at most once, before its container has started.
func (e *kube) WaitStep(ctx context.Context, step *types.Step, taskUUID string) (*types.State, error) {
	podName, err := stepToPodName(step)
	if err != nil {
		return nil, err
	}
	defer e.pendingDeletes.Delete(podName)

	state, err := e.waitStepAttempt(ctx, step, taskUUID)
	var deleted *deletedPodError
	if !errors.As(err, &deleted) {
		var loss *types.PodLossError
		if !errors.As(err, &loss) {
			return state, err
		}
		pending, found := e.pendingDeletes.LoadAndDelete(podName)
		pod, ok := pending.(*kube_core_v1.Pod)
		if !found || !ok || pod.Status.Phase != kube_core_v1.PodPending {
			return state, err
		}
		err = deletedLoss(pod, true, podName, err)
		if !errors.As(err, &deleted) {
			return nil, err
		}
	}

	if _, started := e.started.Load(podName); started || types.InTeardown(ctx) {
		return nil, err
	}

	log.Warn().Str("workflow_id", taskUUID).Str("step", step.Name).Str("pod", podName).Msg("step pod deleted; recreating once; logs of the new pod are not streamed")
	e.started.Delete(podName)
	e.nodes.Delete(podName)
	e.pendingDeletes.Delete(podName)
	if _, startErr := startPod(ctx, e, step, e.stepBackendOptions(step, taskUUID), taskUUID); startErr != nil {
		return nil, errors.Join(err, startErr)
	}
	return e.waitStepAttempt(ctx, step, taskUUID)
}
