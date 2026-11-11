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
	"fmt"
	"sync"
)

var webhookPipelineCreations = newCreationTracker()

// CreationDrainError reports the cause and number of creations still running.
type CreationDrainError struct {
	Cause    error
	InFlight int
}

func (e *CreationDrainError) Error() string {
	return fmt.Sprintf("creation drain interrupted with %d creations still in flight: %v", e.InFlight, e.Cause)
}

func (e *CreationDrainError) Unwrap() error {
	return e.Cause
}

type creationTracker struct {
	mu       sync.Mutex
	draining bool
	inFlight int
	idle     chan struct{}
}

func newCreationTracker() *creationTracker {
	return &creationTracker{}
}

func (t *creationTracker) begin() (done func(), ok bool) {
	t.mu.Lock()
	if t.draining {
		t.mu.Unlock()
		return nil, false
	}
	if t.inFlight == 0 {
		t.idle = make(chan struct{})
	}
	t.inFlight++
	t.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.inFlight--
			if t.inFlight == 0 {
				close(t.idle)
			}
		})
	}, true
}

func (t *creationTracker) startDrain() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.draining = true
	return t.inFlight
}

func (t *creationTracker) drain(ctx context.Context) error {
	t.mu.Lock()
	t.draining = true
	if t.inFlight == 0 {
		t.mu.Unlock()
		return nil
	}
	idle := t.idle
	t.mu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		t.mu.Lock()
		inFlight := t.inFlight
		t.mu.Unlock()
		if inFlight == 0 {
			return nil
		}
		return &CreationDrainError{Cause: context.Cause(ctx), InFlight: inFlight}
	}
}

func (t *creationTracker) inFlightCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inFlight
}

func StartWebhookPipelineCreationDrain() int {
	return webhookPipelineCreations.startDrain()
}

func DrainWebhookPipelineCreations(ctx context.Context) error {
	return webhookPipelineCreations.drain(ctx)
}

func WebhookPipelineCreationsInFlight() int {
	return webhookPipelineCreations.inFlightCount()
}
