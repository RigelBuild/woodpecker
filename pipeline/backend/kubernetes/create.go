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
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
)

// Concurrent creates race on the namespace ResourceQuota's usage update; the apiserver
// answers 409 and the create is safe to repeat. Spread 7 attempts over ~3-5s, since a
// burst of agents all retry together. No Cap: wait.Backoff ends the retries once it is hit.
var quotaConflictBackoff = wait.Backoff{
	Steps:    7,                     //nolint:mnd
	Duration: 50 * time.Millisecond, //nolint:mnd
	Factor:   2,                     //nolint:mnd
	Jitter:   0.5,                   //nolint:mnd
}

// createRetryOnConflict runs create, repeating it only while it returns a conflict.
func createRetryOnConflict[T any](create func() (T, error)) (T, error) {
	var created T
	err := retry.RetryOnConflict(quotaConflictBackoff, func() error {
		var createErr error
		created, createErr = create()
		return createErr
	})
	return created, err
}
