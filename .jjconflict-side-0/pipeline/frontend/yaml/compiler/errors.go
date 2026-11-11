// Copyright 2023 Woodpecker Authors
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

package compiler

import (
	"fmt"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
)

type ErrExtraHostFormat struct {
	host string
}

func (err *ErrExtraHostFormat) Error() string {
	return fmt.Sprintf("extra host %s is in wrong format", err.host)
}

func (*ErrExtraHostFormat) Is(target error) bool {
	_, ok := target.(*ErrExtraHostFormat)
	return ok
}

type ErrStepMissingDependency struct {
	name,
	dep string
}

func (err *ErrStepMissingDependency) Error() string {
	return fmt.Sprintf("step '%s' depends on unknown step '%s'", err.name, err.dep)
}

func (*ErrStepMissingDependency) Is(target error) bool {
	_, ok := target.(*ErrStepMissingDependency)
	return ok
}

type ErrStepFilteredDependency struct {
	name,
	dep string
}

func (err *ErrStepFilteredDependency) Error() string {
	return fmt.Sprintf("step '%s' depends on step '%s' which is filtered out by its conditions", err.name, err.dep)
}

func (*ErrStepFilteredDependency) Is(target error) bool {
	_, ok := target.(*ErrStepFilteredDependency)
	return ok
}

type ErrStepDuplicateName struct {
	name string
}

func (err *ErrStepDuplicateName) Error() string {
	return fmt.Sprintf("step name '%s' is used more than once, step names must be unique to be referenced by 'depends_on'", err.name)
}

func (*ErrStepDuplicateName) Is(target error) bool {
	_, ok := target.(*ErrStepDuplicateName)
	return ok
}

type ErrStepDependencyCycle struct {
	path []string
}

func (err *ErrStepDependencyCycle) Error() string {
	return fmt.Sprintf("cycle detected: %v", err.path)
}

func (*ErrStepDependencyCycle) Is(target error) bool {
	_, ok := target.(*ErrStepDependencyCycle)
	return ok
}

// ErrSecretWithheld marks a gated secret held back until an approver releases it.
// It surfaces as an approval PipelineError so the pipeline blocks instead of failing.
type ErrSecretWithheld struct {
	Name string
}

func (err *ErrSecretWithheld) Error() string {
	return fmt.Sprintf("secret %q is withheld until the pipeline is approved", err.Name)
}

func (*ErrSecretWithheld) Is(target error) bool {
	_, ok := target.(*ErrSecretWithheld)
	return ok
}

func (err *ErrSecretWithheld) As(target any) bool {
	pipelineErr, ok := target.(**pipeline_errors.PipelineError)
	if !ok {
		return false
	}
	*pipelineErr = &pipeline_errors.PipelineError{
		Type:    pipeline_errors.PipelineErrorTypeApproval,
		Message: err.Error(),
		Data:    &pipeline_errors.ApprovalErrorData{Name: err.Name},
	}
	return true
}

// OnlyApprovalErrors reports whether err holds at least one approval error and
// no other blocking error, which is the one case a pipeline may wait for approval.
func OnlyApprovalErrors(err error) bool {
	if err == nil {
		return false
	}
	found := false
	for _, pipelineErr := range pipeline_errors.GetPipelineErrors(err) {
		if pipelineErr.IsWarning {
			continue
		}
		if pipelineErr.Type != pipeline_errors.PipelineErrorTypeApproval {
			return false
		}
		found = true
	}
	return found
}
