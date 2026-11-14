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
	"sync"

	kube_core_v1 "k8s.io/api/core/v1"
)

// observePodUp records what one pod observation says about startup and closes up when the
// pod has started or failed to start. A Running pod is marked started: WaitStep only
// recreates pods that reached Running.
func (e *kube) observePodUp(pod *kube_core_v1.Pod, podName string, up chan struct{}, upOnce *sync.Once) {
	if pod.Name != podName {
		return
	}
	e.recordNode(pod)
	if isImagePullBackOffState(pod) || isInvalidImageName(pod) {
		upOnce.Do(func() { close(up) })
	}
	switch pod.Status.Phase {
	case kube_core_v1.PodRunning:
		e.started.Store(podName, struct{}{})
		upOnce.Do(func() { close(up) })
	case kube_core_v1.PodSucceeded, kube_core_v1.PodFailed:
		upOnce.Do(func() { close(up) })
	}
}
