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
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// The server-wide WriteTimeout is a wall-clock budget on the whole handler, not
// only on a blocked write: net/http arms it once from the request-header read
// (net/http/server.go, the deferred SetWriteDeadline in readRequest), so handler
// compute counts against it too. That is what bounds a write wedged on a
// zero-window peer, but it also caps every legitimately slow response.
//
// A handler that can outrun the budget takes a rolling per-response deadline
// instead, refreshing it as it makes progress. The helpers here are that seam,
// shared by the SSE streams and by the handlers that page against a forge.

// newResponseController builds the ResponseController a handler drives,
// unwrapping past gin's *responseWriter deliberately.
//
// Gin implements the plain http.Flusher (Flush() with no return), so
// ResponseController.Flush() matches its `case Flusher` arm and returns nil
// without ever reaching the wrapped *http.response.FlushError() — every
// flush-error check upstack would be dead code. Unwrapping restores FlushError.
// Gin's rw.Write has already run WriteHeaderNow by then, so header bookkeeping
// and SetWriteDeadline are unaffected. SetWriteDeadline alone would not need
// this, since ResponseController walks Unwrap itself; Flush is what forces it.
//
// The unwrap loops rather than stepping once. One step suffices today, because
// gin's writer is outermost — but a middleware that wraps it (W → gin →
// *http.response) would leave a single step on gin's own writer, whose plain
// Flush() wins the Flusher arm and silently restores the swallowed-flush bug
// this helper exists to prevent. Stopping at the first writer that reports
// flush errors keeps that fix independent of how deep the writer is buried.
//
// A writer with no FlushError anywhere in its chain is used as-is: degraded,
// not broken.
func newResponseController(rw http.ResponseWriter) *http.ResponseController {
	for {
		if _, ok := rw.(interface{ FlushError() error }); ok {
			break
		}
		u, ok := rw.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		rw = u.Unwrap()
	}
	return http.NewResponseController(rw)
}

// extendWriteDeadline pushes the response's write deadline out to now+d,
// replacing whatever the server-wide WriteTimeout armed. It is the primitive
// behind both rolling-deadline shapes: a stream re-arms before each write, a
// slow handler re-arms as it makes progress.
//
// A writer that does not support SetWriteDeadline is reported at Warn, not
// Debug: it means the per-response bound is not in force and the handler is
// running under the server-wide deadline alone, which is the condition this
// whole mechanism exists to avoid. It is not fatal — the server-wide deadline
// still reclaims the connection — but an operator should not need debug logging
// to discover the mitigation is inactive.
//
// The warnOnce keeps that Warn to one line per response, and is required rather
// than optional because every caller arms repeatedly against one response: a
// stream before each write, a slow handler on each unit of progress, the queue
// long-poll at 10Hz. The condition is a property of the writer and cannot
// change mid-response, so repeating the line adds no information and floods
// the log for as long as the request lives.
func extendWriteDeadline(rc *http.ResponseController, d time.Duration, warnOnce *sync.Once) {
	err := rc.SetWriteDeadline(time.Now().Add(d))
	if err == nil {
		return
	}
	warnOnce.Do(func() {
		log.Warn().Err(err).Dur("extension", d).
			Msg("SetWriteDeadline unsupported: no per-response write bound, " +
				"handler runs under the server-wide WriteTimeout alone")
	})
}

// slowHandlerProgress returns the per-iteration hook a slow handler runs as it
// makes progress: it re-arms the rolling write deadline and reports whether the
// client is still there.
//
// Both halves are required together, and that is the point of pairing them.
// Re-arming removes the server-wide WriteTimeout as a bound, and a handler that
// pages against a forge writes nothing while it works — so no write can trip
// the deadline and nothing else would ever end the loop. Cancellation is the
// bound that replaces the one the arming removed.
func slowHandlerProgress(c *gin.Context, rc *http.ResponseController) func() error {
	requestCtx := c.Request.Context()
	var warnOnce sync.Once
	return func() error {
		if err := requestCtx.Err(); err != nil {
			return err
		}
		extendWriteDeadline(rc, slowHandlerWriteExtension, &warnOnce)
		return nil
	}
}

// statusClientClosedRequest is nginx's 499: the client closed the connection
// before the server produced a response. Not an IANA code, so net/http has no
// constant for it, but gin writes it verbatim and it is what the access log
// needs to distinguish an abandoned request from a completed one. A handler
// whose slowHandlerProgress hook reports cancellation has done partial work
// nobody will read; without an explicit status gin finalizes a bare 200 and
// the log records that partial work as a success.
const statusClientClosedRequest = 499
