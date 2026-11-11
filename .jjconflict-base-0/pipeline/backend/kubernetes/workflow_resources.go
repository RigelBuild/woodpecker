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
	hasLimits := make(map[string]bool)

	for _, stage := range conf.Stages {
		for _, step := range stage.Steps {
			if step.Type == types.StepTypeClone || step.Type == types.StepTypeService {
				continue
			}

			options, err := parseBackendOptions(step)
			if err != nil {
				log.Warn().Err(err).Str("step", step.Name).Msg("skipping step when sizing the clone")
				continue
			}
			stepRequests := make(map[string]resource.Quantity, len(options.Resources.Requests))
			stepLimits := make(map[string]resource.Quantity, len(options.Resources.Limits))
			if err := parseResourceQuantities(stepRequests, options.Resources.Requests, step.Name, "requests"); err != nil {
				log.Warn().Err(err).Msg("skipping step when sizing the clone")
				continue
			}
			if err := parseResourceQuantities(stepLimits, options.Resources.Limits, step.Name, "limits"); err != nil {
				log.Warn().Err(err).Msg("skipping step when sizing the clone")
				continue
			}

			for name, quantity := range stepRequests {
				mergeQuantityMax(requests, name, quantity)
			}
			for name, quantity := range stepLimits {
				hasLimits[name] = true
				mergeQuantityMax(limits, name, quantity)
				if _, hasRequest := stepRequests[name]; !hasRequest {
					mergeQuantityMax(requests, name, quantity)
				}
			}
		}
	}

	// Requests drive scheduling, so every effective request is kept. A limit is set only
	// where some step limits that resource, raised so it never falls below the request.
	resources := Resources{}
	if len(requests) > 0 {
		resources.Requests = make(map[string]string, len(requests))
		for name, request := range requests {
			resources.Requests[name] = request.String()
			if hasLimits[name] {
				mergeQuantityMax(limits, name, request)
			}
		}
	}
	if len(limits) > 0 {
		resources.Limits = make(map[string]string, len(limits))
		for name, limit := range limits {
			resources.Limits[name] = limit.String()
		}
	}

	return resources
}

func parseResourceQuantities(parsed map[string]resource.Quantity, values map[string]string, stepName, field string) error {
	for name, value := range values {
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return fmt.Errorf("resource %s for step %q, quantity %q: %w", field, stepName, value, err)
		}
		parsed[name] = quantity
	}
	return nil
}

func mergeQuantityMax(maxima map[string]resource.Quantity, name string, quantity resource.Quantity) {
	current, ok := maxima[name]
	if !ok || quantity.Cmp(current) > 0 {
		maxima[name] = quantity.DeepCopy()
	}
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
