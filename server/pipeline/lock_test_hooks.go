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

//go:build test

package pipeline

// SetLockWaitHook installs a hook that runs before a caller waits for a pipeline
// lock; nil removes it. Only test builds can reach it.
func SetLockWaitHook(hook func(pipelineID int64)) {
	if hook == nil {
		lockWaitHook.Store(nil)
		return
	}
	lockWaitHook.Store(&hook)
}
