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

package pipeline

import (
	"sync"
	"sync/atomic"
)

// pipelineLocks serializes Approve, Cancel and Decline per pipeline. A
// process-local lock is enough because the server runs as a single instance.
var pipelineLocks = keyedMutex{entries: map[int64]*keyedEntry{}}

// lockWaitHook, when set, runs when a caller finds a pipeline lock held and is
// about to wait, so tests can order a concurrent Cancel without sleeping.
var lockWaitHook atomic.Pointer[func(pipelineID int64)]

type keyedMutex struct {
	mu      sync.Mutex
	entries map[int64]*keyedEntry
}

type keyedEntry struct {
	mu   sync.Mutex
	refs int
}

// lock takes the lock for one pipeline and returns its unlock. The entry is
// dropped once no caller holds or waits for it, so the map does not grow.
func (k *keyedMutex) lock(pipelineID int64) (unlock func()) {
	k.mu.Lock()
	entry, ok := k.entries[pipelineID]
	if !ok {
		entry = &keyedEntry{}
		k.entries[pipelineID] = entry
	}
	entry.refs++
	k.mu.Unlock()

	if !entry.mu.TryLock() {
		if hook := lockWaitHook.Load(); hook != nil {
			(*hook)(pipelineID)
		}
		entry.mu.Lock()
	}

	return func() {
		entry.mu.Unlock()
		k.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(k.entries, pipelineID)
		}
		k.mu.Unlock()
	}
}
