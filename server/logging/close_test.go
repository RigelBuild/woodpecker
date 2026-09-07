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

package logging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Closing a step that never opened a stream is expected: a step killed or
// skipped before it ran has nothing to close. Callers tell that apart from a
// real failure with errors.Is(err, ErrNotFound), so Close must return the
// sentinel unwrapped rather than a same-text error.
func TestCloseUnopenedStreamReturnsErrNotFound(t *testing.T) {
	t.Parallel()

	err := New().Close(t.Context(), int64(123))

	assert.ErrorIs(t, err, ErrNotFound)
}

// Closing an open stream succeeds, so a caller that treats every Close error
// as expected would hide a genuine failure. The two cases must stay distinct.
func TestCloseOpenStreamSucceeds(t *testing.T) {
	t.Parallel()

	stepID := int64(456)
	logger := New()
	assert.NoError(t, logger.Open(t.Context(), stepID))

	assert.NoError(t, logger.Close(t.Context(), stepID))

	// Closing the same stream again finds it already retired. This holds for
	// sequential calls; Close drops the lock between the lookup and the
	// delete, so concurrent calls are a separate matter.
	assert.ErrorIs(t, logger.Close(t.Context(), stepID), ErrNotFound)
}

// Tail shares the same not-found contract as Close.
func TestTailUnopenedStreamReturnsErrNotFound(t *testing.T) {
	t.Parallel()

	receiver := make(LogChan, 1)
	err := New().Tail(t.Context(), int64(789), receiver)

	assert.ErrorIs(t, err, ErrNotFound)
}
