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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	lockTestTimeout = 5 * time.Second
	lockTestTick    = time.Millisecond
)

func TestKeyedMutexSerializesOneKeyAndDropsUnusedEntries(t *testing.T) {
	k := keyedMutex{entries: map[int64]*keyedEntry{}}

	unlockA := k.lock(1)
	unlockOther := k.lock(2) // a different pipeline never waits on pipeline 1
	unlockOther()

	acquired := make(chan struct{})
	go func() {
		unlock := k.lock(1)
		close(acquired)
		unlock()
	}()

	// The second caller registers before it waits, so its entry outlives unlockA.
	assert.Eventually(t, func() bool {
		k.mu.Lock()
		defer k.mu.Unlock()
		return k.entries[1] != nil && k.entries[1].refs == 2
	}, lockTestTimeout, lockTestTick)
	select {
	case <-acquired:
		t.Fatal("a second lock on the same pipeline must wait for the first")
	default:
	}

	unlockA()
	<-acquired
	assert.Eventually(t, func() bool {
		k.mu.Lock()
		defer k.mu.Unlock()
		return len(k.entries) == 0
	}, lockTestTimeout, lockTestTick, "entries must be removed once unused")
}
