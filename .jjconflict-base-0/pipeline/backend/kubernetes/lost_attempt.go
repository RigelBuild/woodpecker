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
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	kube_meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// lostAttemptPollInterval spaces the checks while the old pods finish their grace period.
var lostAttemptPollInterval = time.Second

// recreateAfterLostAttempt deletes every object an earlier attempt of this workflow left,
// then creates a fresh workflow volume. A requeued workflow reuses all these names.
func (e *kube) recreateAfterLostAttempt(ctx context.Context, conf *types.Config, namespace, taskUUID string) error {
	opts := e.config.newDefaultDeleteOptions()
	var podNames []string
	for _, stage := range conf.Stages {
		for _, step := range stage.Steps {
			if err := stopPod(ctx, e, step, opts); err != nil {
				return err
			}
			if needsStepSecret(step) {
				if err := stopStepSecret(ctx, e, step, opts); err != nil {
					return err
				}
			}
			if needsRegistrySecret(step) {
				if err := stopRegistrySecret(ctx, e, step, opts); err != nil {
					return err
				}
			}
			name, err := stepToPodName(step)
			if err != nil {
				return err
			}
			podNames = append(podNames, name)
		}
	}
	if err := e.stopHeadlessService(ctx, e, namespace, taskUUID); err != nil {
		return err
	}
	if err := stopVolume(ctx, e, conf.Volume, namespace, opts); err != nil {
		return err
	}

	// The PVC protection finalizer holds the claim until the old pods are gone. The agent
	// Role grants no get on PVCs, so retry the create until the old claim stops blocking it.
	timeout := time.Duration(e.config.StopTimeout)*time.Second + time.Minute
	return wait.PollUntilContextTimeout(ctx, lostAttemptPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		for _, name := range podNames {
			if gone, err := isGone(e.client.CoreV1().Pods(namespace).Get(ctx, name, kube_meta_v1.GetOptions{})); !gone {
				return false, err
			}
		}
		err := startVolume(ctx, e, conf.Volume, namespace)
		if errors.IsAlreadyExists(err) {
			return false, nil
		}
		return err == nil, err
	})
}

// isGone turns a Get result into a poll result: done on NotFound, failed on other errors.
func isGone[T any](_ T, err error) (bool, error) {
	if errors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}
