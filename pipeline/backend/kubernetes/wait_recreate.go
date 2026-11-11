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

	"github.com/rs/zerolog/log"
	kube_core_v1 "k8s.io/api/core/v1"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// deletedPodError marks a loss where the pod object was deleted, not left behind.
// Only this loss is eligible for a recreate; an evicted pod whose object remains is not.
type deletedPodError struct {
	err error
}

func (e *deletedPodError) Error() string { return e.err.Error() }
func (e *deletedPodError) Unwrap() error { return e.err }

// deletedLoss marks a loss recreatable only when the last snapshot shows the step container
// still running or waiting. A nil snapshot, or any terminated state, is not recreated:
// the step already ran, or nothing shows the pod was ever scheduled.
func deletedLoss(last *kube_core_v1.Pod, podName string, err error) error {
	if last == nil {
		return err
	}
	container, found := stepContainerStatus(last, podName)
	if !found || container.State.Terminated != nil {
		return err
	}
	if container.State.Running == nil && container.State.Waiting == nil {
		return err
	}
	return &deletedPodError{err: err}
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

// WaitStep waits for the step pod to finish. If the pod was deleted after it reached Running,
// it recreates the pod once and waits again. A second loss fails the step.
// A pod lost before it reached Running is reported without a recreate.
func (e *kube) WaitStep(ctx context.Context, step *types.Step, taskUUID string) (*types.State, error) {
	podName, err := stepToPodName(step)
	if err != nil {
		return nil, err
	}

	state, err := e.waitStepAttempt(ctx, step, taskUUID)
	if !isDeletedLoss(err) {
		return state, err
	}
	if _, started := e.started.Load(podName); !started || types.InTeardown(ctx) {
		return nil, err
	}

	log.Warn().Str("workflow_id", taskUUID).Str("step", step.Name).Str("pod", podName).Msg("step pod deleted after start; recreating once; logs of the new pod are not streamed")
	e.started.Delete(podName)
	e.nodes.Delete(podName)
	if _, startErr := startPod(ctx, e, step, e.stepBackendOptions(step, taskUUID), taskUUID); startErr != nil {
		return nil, errors.Join(err, startErr)
	}
	return e.waitStepAttempt(ctx, step, taskUUID)
}

func isDeletedLoss(err error) bool {
	var deleted *deletedPodError
	return errors.As(err, &deleted)
}
