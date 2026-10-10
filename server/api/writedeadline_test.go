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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// deadlineRecorder is a ResponseWriter that records SetWriteDeadline calls.
// It deliberately does NOT implement Flush, so a controller built on it
// reports whether the unwrap reached this writer or stopped at a wrapper.
type deadlineRecorder struct {
	http.ResponseWriter
	deadlines []time.Time
	err       error
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	if d.err != nil {
		return d.err
	}
	d.deadlines = append(d.deadlines, t)
	return nil
}

// flushWrapper mimics gin's *responseWriter: it implements the plain
// http.Flusher (Flush with no error return) and exposes the wrapped writer
// through Unwrap. A ResponseController built on this rather than on its
// unwrapped target matches the Flusher arm and silently swallows flush errors.
type flushWrapper struct {
	http.ResponseWriter
	flushed bool
}

func (f *flushWrapper) Flush()                      { f.flushed = true }
func (f *flushWrapper) Unwrap() http.ResponseWriter { return f.ResponseWriter }

func TestNewResponseControllerUnwrapsPastAFlusher(t *testing.T) {
	// The wrapper implements Flush; the target does not. If the controller is
	// built on the wrapper it matches the Flusher arm and returns nil. Built on
	// the unwrapped target it finds neither FlushError nor Flusher and reports
	// ErrNotSupported. That difference is the whole reason for the unwrap, so
	// asserting it is what keeps the unwrap from being quietly removed.
	target := &deadlineRecorder{}
	wrapper := &flushWrapper{ResponseWriter: target}

	err := newResponseController(wrapper).Flush()
	assert.ErrorIs(t, err, http.ErrNotSupported,
		"controller must drive the unwrapped writer, not the wrapper's own Flush")
	assert.False(t, wrapper.flushed,
		"the wrapper's Flush must not be what runs")

	// Built on the wrapper directly, the swallowed-flush behavior appears —
	// this is the bug the unwrap avoids, pinned so the two paths must differ.
	assert.NoError(t, http.NewResponseController(wrapper).Flush())
	assert.True(t, wrapper.flushed)
}

func TestNewResponseControllerHandlesAWriterWithoutUnwrap(t *testing.T) {
	// Degraded, not broken: a writer that cannot be unwrapped is used as-is.
	target := &deadlineRecorder{}

	before := time.Now()
	extendWriteDeadline(newResponseController(target), time.Minute, &sync.Once{})

	if assert.Len(t, target.deadlines, 1) {
		assert.WithinRange(t, target.deadlines[0],
			before.Add(time.Minute), time.Now().Add(time.Minute))
	}
}

func TestExtendWriteDeadlineSurvivesAnUnsupportedWriter(t *testing.T) {
	// A writer that cannot take a deadline must not panic or block the caller:
	// the handler keeps running under the server-wide deadline instead.
	target := &deadlineRecorder{err: http.ErrNotSupported}

	assert.NotPanics(t, func() {
		extendWriteDeadline(newResponseController(target), time.Minute, &sync.Once{})
	})
	assert.Empty(t, target.deadlines)
}

func TestArmSSEWriteDeadlineClampsToTheCeiling(t *testing.T) {
	// The ceiling is what reclaims a ping-only stream, whose writes never block
	// and so never trip the rolling deadline on their own.
	for _, tc := range []struct {
		name      string
		remaining time.Duration
		wantMax   time.Duration
	}{
		{"far from the ceiling: the full rolling arm", time.Hour, 2 * idlePingTime},
		{"near the ceiling: clamped to what remains", idlePingTime / 2, idlePingTime},
		{"past the ceiling: floored, never in the past", -time.Minute, idlePingTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := &deadlineRecorder{}
			before := time.Now()

			armSSEWriteDeadline(newResponseController(target), before.Add(tc.remaining), &sync.Once{})

			if assert.Len(t, target.deadlines, 1) {
				got := target.deadlines[0]
				assert.False(t, got.Before(before),
					"an arm must never set a deadline in the past: %v", got)
				assert.LessOrEqual(t, got.Sub(before), tc.wantMax+time.Second,
					"arm must not exceed the clamp")
			}
		})
	}
}

// newSlowHandlerCtx builds a gin context whose request carries the given
// context, plus a ResponseController over a deadlineRecorder, mirroring the
// construction the other tests here use (gin.CreateTestContext +
// newResponseController over a deadline-recording writer).
func newSlowHandlerCtx(reqCtx context.Context) (*gin.Context, *http.ResponseController, *deadlineRecorder) {
	target := &deadlineRecorder{}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil).WithContext(reqCtx)
	return c, newResponseController(target), target
}

func TestSlowHandlerProgressContinuesWhileTheClientIsThere(t *testing.T) {
	// The live path: a still-connected client means the hook re-arms the
	// rolling deadline and returns nil, so the paging loop keeps going. This is
	// the branch the four handlers do NOT translate to 499 — a false positive
	// here would abort a healthy request as a client disconnect.
	c, rc, target := newSlowHandlerCtx(context.Background())

	hook := slowHandlerProgress(c, rc)

	before := time.Now()
	assert.NoError(t, hook(), "a live request context must not end the paging loop")
	if assert.Len(t, target.deadlines, 1, "the hook must re-arm the write deadline each call") {
		assert.WithinRange(t, target.deadlines[0],
			before.Add(slowHandlerWriteExtension), time.Now().Add(slowHandlerWriteExtension))
	}

	// Each call re-arms afresh: a slow handler leans on this per-iteration.
	assert.NoError(t, hook())
	assert.Len(t, target.deadlines, 2)
}

func TestSlowHandlerProgressStopsOnCancellation(t *testing.T) {
	// The cancel path: once the request context reports an error the hook
	// returns it verbatim, and the error must be Is-matchable so the handlers
	// can distinguish a client disconnect (→499, stop paging) from a genuine
	// forge failure (→500). The hook must also NOT re-arm the deadline once
	// canceled — it bails before the extend.
	for _, tc := range []struct {
		name    string
		arm     func() context.Context
		wantErr error
	}{
		{
			"client hang-up: context.Canceled",
			func() context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(nil)
				return ctx
			},
			context.Canceled,
		},
		{
			"budget blown: context.DeadlineExceeded",
			func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
				t.Cleanup(cancel)
				return ctx
			},
			context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The context returned by arm is already terminal (canceled now, or
			// past its deadline by construction), so no sleeps are needed.
			reqCtx := tc.arm()

			c, rc, target := newSlowHandlerCtx(reqCtx)
			err := slowHandlerProgress(c, rc)()

			assert.Error(t, err, "a canceled request context must end the paging loop")
			assert.ErrorIs(t, err, tc.wantErr,
				"the error must be Is-matchable so handlers can translate it to 499")
			assert.Empty(t, target.deadlines,
				"a canceled hook must bail before re-arming the write deadline")
		})
	}
}
