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

package api

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestCreationTracker(t *testing.T) {
	t.Run("refuses new creations after drain start", func(t *testing.T) {
		tracker := newCreationTracker()
		if inFlight := tracker.startDrain(); inFlight != 0 {
			t.Fatalf("initial in-flight = %d, want 0", inFlight)
		}
		if _, ok := tracker.begin(); ok {
			t.Fatal("begin accepted a creation after drain start")
		}
	})

	// synctest.Wait returns once drain is durably blocked, so "still waiting" is deterministic.
	assertDrainWaitsFor := func(t *testing.T, tracker *creationTracker, done func()) {
		t.Helper()
		finished := make(chan error, 1)
		go func() {
			finished <- tracker.drain(t.Context())
		}()
		synctest.Wait()
		select {
		case err := <-finished:
			t.Fatalf("drain returned before creation finished: %v", err)
		default:
		}
		done()
		if err := <-finished; err != nil {
			t.Fatalf("drain: %v", err)
		}
	}

	t.Run("drain waits for active creation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			tracker := newCreationTracker()
			done, ok := tracker.begin()
			if !ok {
				t.Fatal("begin refused before drain")
			}
			tracker.startDrain()
			if _, ok := tracker.begin(); ok {
				t.Fatal("begin accepted a creation while draining")
			}
			assertDrainWaitsFor(t, tracker, done)
		})
	})

	t.Run("drain waits for a creation started after an earlier one finished", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			tracker := newCreationTracker()
			first, ok := tracker.begin()
			if !ok {
				t.Fatal("begin refused before drain")
			}
			first()
			second, ok := tracker.begin()
			if !ok {
				t.Fatal("begin refused before drain")
			}
			tracker.startDrain()
			assertDrainWaitsFor(t, tracker, second)
		})
	})

	t.Run("deadline reports remaining creations", func(t *testing.T) {
		tracker := newCreationTracker()
		done, ok := tracker.begin()
		if !ok {
			t.Fatal("begin refused before drain")
		}
		defer done()
		ctx, cancel := context.WithCancelCause(t.Context())
		cause := errors.New("drain deadline")
		cancel(cause)
		err := tracker.drain(ctx)
		var drainErr *CreationDrainError
		if !errors.As(err, &drainErr) || !errors.Is(err, cause) {
			t.Fatalf("drain error = %v, want cause %v", err, cause)
		}
		if drainErr.InFlight != 1 {
			t.Fatalf("in-flight = %d, want 1", drainErr.InFlight)
		}
	})

	t.Run("deadline context returns with active creation", func(t *testing.T) {
		tracker := newCreationTracker()
		done, ok := tracker.begin()
		if !ok {
			t.Fatal("begin refused before drain")
		}
		defer done()
		ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()
		err := tracker.drain(ctx)
		var drainErr *CreationDrainError
		if !errors.As(err, &drainErr) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drain error = %v, want deadline exceeded", err)
		}
		if drainErr.InFlight != 1 {
			t.Fatalf("in-flight = %d, want 1", drainErr.InFlight)
		}
	})

	t.Run("done is idempotent", func(t *testing.T) {
		tracker := newCreationTracker()
		done, ok := tracker.begin()
		if !ok {
			t.Fatal("begin refused before drain")
		}
		done()
		done()
		if got := tracker.inFlightCount(); got != 0 {
			t.Fatalf("in-flight = %d, want 0", got)
		}
	})
}
