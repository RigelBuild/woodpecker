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

package types

import (
	"errors"
	"fmt"
)

var ErrNoCliContextFound = errors.New("no CliContext in context found")

// ErrStepPodLost marks a step pod that disappeared before it completed.
var ErrStepPodLost = errors.New("step pod lost")

// PodLossError describes a step pod lost to an unexpected deletion or disruption.
type PodLossError struct {
	Pod, Node string
	Reason    string
	Confirmed bool
	Evidence  string
}

func (e *PodLossError) Error() string {
	return fmt.Sprintf("%s: pod %s on node %s: %s", ErrStepPodLost.Error(), e.Pod, e.Node, e.Reason)
}

func (e *PodLossError) Unwrap() error {
	return ErrStepPodLost
}
