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

package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// A restart must not reap before one interval has passed: the first pass
// would otherwise see every workflow persisted by the previous process at once.
func TestRunOrphanReaperWaitsOneTickBeforeFirstPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		ticks := make(chan time.Time)
		passes := make(chan struct{}, 8)
		done := make(chan struct{})
		go func() {
			defer close(done)
			runOrphanReaper(ctx, ticks, func() error {
				passes <- struct{}{}
				return nil
			})
		}()

		synctest.Wait()
		assert.Len(t, passes, 0, "reaper ran before the first tick")

		ticks <- time.Now()
		synctest.Wait()
		assert.Len(t, passes, 1, "one tick should run exactly one pass")

		cancel()
		<-done
	})
}
