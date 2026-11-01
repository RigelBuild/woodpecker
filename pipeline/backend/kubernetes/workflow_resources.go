// Copyright 2026 Woodpecker Authors
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

package kubernetes

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/api/resource"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

func maxWorkflowResources(conf *types.Config) Resources {
	requests := make(map[string]resource.Quantity)
	limits := make(map[string]resource.Quantity)

	for _, stage := range conf.Stages {
		for _, step := range stage.Steps {
			if step.Type == types.StepTypeClone || step.Type == types.StepTypeService {
				continue
			}

			// StartStep only logs a bad options block, so an unparseable step must not fail setup.
			options, err := parseBackendOptions(step)
			if err != nil {
				log.Warn().Err(err).Str("step", step.Name).Msg("skipping step when sizing the clone")
				continue
			}
			if err := mergeResourceMaxima(requests, options.Resources.Requests, step.Name, "requests"); err != nil {
				log.Warn().Err(err).Msg("skipping step when sizing the clone")
				continue
			}
			if err := mergeResourceMaxima(limits, options.Resources.Limits, step.Name, "limits"); err != nil {
				log.Warn().Err(err).Msg("skipping step when sizing the clone")
			}
		}
	}

	resources := Resources{}
	if len(requests) > 0 {
		resources.Requests = make(map[string]string, len(requests))
		for name, quantity := range requests {
			resources.Requests[name] = quantity.String()
		}
	}
	if len(limits) > 0 {
		resources.Limits = make(map[string]string, len(limits))
		for name, quantity := range limits {
			resources.Limits[name] = quantity.String()
		}
	}

	return resources
}

func mergeResourceMaxima(maxima map[string]resource.Quantity, values map[string]string, stepName, field string) error {
	for name, value := range values {
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return fmt.Errorf("resource %s for step %q, quantity %q: %w", field, stepName, value, err)
		}
		current, ok := maxima[name]
		if !ok || quantity.Cmp(current) > 0 {
			maxima[name] = quantity.DeepCopy()
		}
	}
	return nil
}

func (e *kube) setWorkflowResources(taskUUID string, resources Resources) {
	e.workflowResourcesMu.Lock()
	defer e.workflowResourcesMu.Unlock()

	if e.workflowResources == nil {
		e.workflowResources = make(map[string]Resources)
	}
	e.workflowResources[taskUUID] = resources
}

func (e *kube) getWorkflowResources(taskUUID string) Resources {
	e.workflowResourcesMu.RLock()
	defer e.workflowResourcesMu.RUnlock()

	return e.workflowResources[taskUUID]
}

func (e *kube) deleteWorkflowResources(taskUUID string) {
	e.workflowResourcesMu.Lock()
	defer e.workflowResourcesMu.Unlock()

	delete(e.workflowResources, taskUUID)
}
