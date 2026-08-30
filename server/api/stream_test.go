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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestEventStreamSSEConcurrentDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	broker := memory.New()
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, nil, broker)
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	for i := range 50 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			ctx, cancel := context.WithCancelCause(t.Context())
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/stream/events", nil)
			c.Request = req

			topic := map[string]struct{}{pubsub.PublicTopic: {}}

			done := make(chan struct{})
			go func() {
				defer close(done)
				EventStreamSSE(c)
			}()

			// Let the event handler subscribe
			time.Sleep(20 * time.Millisecond)

			// Fire concurrent publishes while canceling the request.
			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = broker.Publish(ctx, topic, pubsub.Message{
						Data: []byte(`{"pipeline":1}`),
					})
				}()
			}

			// Simulate client disconnect mid-publish.
			cancel(nil)
			wg.Wait()
			<-done
		})
	}
}

func setupLogStreamContext(t *testing.T) (*httptest.ResponseRecorder, *gin.Context, context.CancelCauseFunc) {
	t.Helper()

	const stepID int64 = 42
	const pipelineID int64 = 10

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("GetPipelineNumber", mock.Anything, mock.Anything).
		Return(&model.Pipeline{ID: pipelineID}, nil)
	mockStore.On("StepLoad", mock.Anything, mock.Anything).
		Return(&model.Step{
			ID:         stepID,
			PipelineID: pipelineID,
			State:      model.StatusRunning,
		}, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	ctx, cancel := context.WithCancelCause(t.Context())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/stream/logs/1/1/42", nil)
	c.Request = req
	c.Params = gin.Params{
		{Key: "repo_id", Value: "1"},
		{Key: "pipeline", Value: "1"},
		{Key: "step_id", Value: "42"},
	}
	c.Set("repo", &model.Repo{ID: 1, FullName: "owner/repo"})
	c.Set("store", mockStore)

	return w, c, cancel
}

func TestLogStreamSSEConcurrentDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logService := logging.New()
	server.Config.Services.Logs = logService
	t.Cleanup(func() { server.Config.Services.Logs = nil })

	const stepID int64 = 42

	for i := range 50 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			done := make(chan struct{})

			_, c, cancel := setupLogStreamContext(t)

			go func() {
				defer close(done)
				LogStreamSSE(c)
			}()

			// Let LogStreamSSE open the stream and start tailing.
			time.Sleep(20 * time.Millisecond)

			// Fire concurrent log writes while canceling the request.
			var wg sync.WaitGroup
			for i := range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = logService.Write(t.Context(), stepID, []*model.LogEntry{
						{Line: i, Data: []byte("log line")},
					})
				}()
			}

			// Simulate client disconnect mid-write.
			cancel(nil)
			wg.Wait()
			<-done
		})
	}
}

// newSSEEventServer wires the real EventStreamSSE handler behind a gin router,
// backed by an in-memory pubsub broker, and returns a running httptest server.
// The handlerDone channel is closed when the handler returns; start selects the
// transport (plain HTTP/1 or HTTP/2+TLS) so the same assertion runs over both.
func newSSEEventServer(
	t *testing.T,
	writeTimeout time.Duration,
	start func(*httptest.Server),
) (ts *httptest.Server, broker pubsub.PubSub, handlerDone <-chan struct{}) {
	t.Helper()

	broker = memory.New()
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, nil, broker)
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	done := make(chan struct{})
	router := gin.New()
	router.GET("/stream/events", func(c *gin.Context) {
		defer close(done)
		EventStreamSSE(c)
	})

	ts = httptest.NewUnstartedServer(router)
	ts.Config.WriteTimeout = writeTimeout
	start(ts)
	t.Cleanup(ts.Close)

	return ts, broker, done
}

// TestEventStreamSSESurvivesWriteTimeout pins the rolling-deadline override: a
// stream keeps delivering events well past the server-wide WriteTimeout,
// because the handler re-arms a rolling per-response write deadline
// (2*idlePingTime) before every write. It runs over both HTTP/1 and HTTP/2+TLS
// so the h2 responseWriter's SetWriteDeadline path is exercised too. Without
// the rolling deadline the server would tear the stream down at WriteTimeout
// and no event would arrive after it — the assertion below would fail.
func TestEventStreamSSESurvivesWriteTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// The override proof needs the rolling deadline (2*idlePingTime) to outlast
	// the server-wide WriteTimeout on the FIRST arm, so the stream survives past
	// it even if the handler never gets to re-arm — otherwise survival would hinge
	// on re-arming within the window every time, which a scheduler stall under
	// load can miss (flaky). Keep 2*idlePingTime (200ms) > testWriteTimeout (80ms)
	// with margin. Mutates the shared idlePingTime global, so no t.Parallel().
	origPing := idlePingTime
	idlePingTime = 100 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	const testWriteTimeout = 80 * time.Millisecond

	transports := []struct {
		name  string
		start func(*httptest.Server)
	}{
		{"http1", func(ts *httptest.Server) { ts.Start() }},
		{"http2", func(ts *httptest.Server) { ts.EnableHTTP2 = true; ts.StartTLS() }},
	}

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			ts, broker, handlerDone := newSSEEventServer(t, testWriteTimeout, tr.start)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/events", nil)
			require.NoError(t, err)
			resp, err := ts.Client().Do(req)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, http.StatusOK, resp.StatusCode)

			// Emit data events continuously, past the WriteTimeout window.
			pubStop := make(chan struct{})
			go func() {
				ticker := time.NewTicker(30 * time.Millisecond)
				defer ticker.Stop()
				topic := map[string]struct{}{pubsub.PublicTopic: {}}
				for {
					select {
					case <-pubStop:
						return
					case <-ticker.C:
						_ = broker.Publish(context.Background(), topic,
							pubsub.Message{Data: []byte(`{"pipeline":1}`)})
					}
				}
			}()
			t.Cleanup(func() { close(pubStop) })

			// Read the stream and confirm at least one line arrives strictly
			// after WriteTimeout has elapsed — proof the rolling deadline
			// overrode the server-wide one.
			start := time.Now()
			br := bufio.NewReader(resp.Body)
			readDeadline := time.Now().Add(2*testWriteTimeout + time.Second)
			var gotLate bool
			for time.Now().Before(readDeadline) {
				line, err := br.ReadString('\n')
				if err != nil {
					break
				}
				if strings.TrimSpace(line) == "" {
					continue
				}
				if time.Since(start) > testWriteTimeout {
					gotLate = true
					break
				}
			}
			require.True(t, gotLate,
				"SSE stream stopped delivering before WriteTimeout elapsed; the rolling write deadline did not override the server-wide WriteTimeout")

			// Deterministically reap the handler goroutine before this subtest
			// returns. Canceling the request trips the handler's
			// requestCtx.Done() arm; waiting on handlerDone establishes a
			// happens-before with the parent's idlePingTime restore, so the
			// live handler can no longer race the restore's write of the
			// global. This is the fix for that race — not a reason to rerun
			// the test until it passes.
			cancel()
			select {
			case <-handlerDone:
			case <-time.After(2*testWriteTimeout + time.Second):
				t.Fatal("SSE handler did not return after request cancel; goroutine leaked")
			}
		})
	}
}

// TestEventStreamSSEWedgeKill pins the return-on-write-error path: when a
// client stops reading, the send buffer fills, and the blocked write is
// reclaimed by the rolling deadline (2*idlePingTime) — the handler observes the
// write error and returns instead of spinning forever. WriteTimeout is left 0
// so the ONLY thing that can unblock the wedged write is the per-response
// deadline the fix arms; without it the handler would block indefinitely and
// this test would time out.
func TestEventStreamSSEWedgeKill(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origPing := idlePingTime
	idlePingTime = 40 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	// WriteTimeout 0: no server-wide deadline. Only armSSEWriteDeadline can
	// reclaim the wedged write.
	ts, broker, handlerDone := newSSEEventServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/events", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The client deliberately never reads resp.Body. Flood large payloads so
	// the kernel send buffer fills and the next handler write blocks.
	pubStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		topic := map[string]struct{}{pubsub.PublicTopic: {}}
		big := bytes.Repeat([]byte("x"), 256<<10)
		for {
			select {
			case <-pubStop:
				return
			case <-ticker.C:
				_ = broker.Publish(context.Background(), topic, pubsub.Message{Data: big})
			}
		}
	}()
	t.Cleanup(func() { close(pubStop) })

	// The wedged write must be reclaimed by the rolling deadline and the
	// handler must return within 2*idlePingTime + slack.
	const slack = 3 * time.Second
	select {
	case <-handlerDone:
		// handler returned — the write error was surfaced and acted on.
	case <-time.After(2*idlePingTime + slack):
		t.Fatalf("SSE handler did not return within 2*idlePingTime+slack (%s); "+
			"the wedged write was never reclaimed / the write error was not acted on", 2*idlePingTime+slack)
	}
}

// newSSELogServer wires the real LogStreamSSE handler behind a gin router,
// backed by an in-memory logging service, and returns a running httptest
// server. Unlike setupLogStreamContext (which uses an httptest.NewRecorder that
// never blocks), this drives the handler over a real net.Conn so a real write
// can actually block and the rolling write deadline can fire. The route wrapper
// populates the context the way middleware would before calling LogStreamSSE:
// it sets the mock store (GetPipelineNumber -> Pipeline{ID:pipelineID},
// StepLoad -> running Step{ID:stepID}) and repo, matching the route params
// repo_id=1/pipeline=1/step_id=42. The handlerDone channel is closed when the
// handler returns; start selects the transport (plain HTTP/1 or HTTP/2+TLS).
func newSSELogServer(
	t *testing.T,
	writeTimeout time.Duration,
	start func(*httptest.Server),
) (ts *httptest.Server, logService logging.Log, handlerDone <-chan struct{}) {
	t.Helper()

	const stepID int64 = 42
	const pipelineID int64 = 10

	logService = logging.New()
	server.Config.Services.Logs = logService
	t.Cleanup(func() { server.Config.Services.Logs = nil })

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("GetPipelineNumber", mock.Anything, mock.Anything).
		Return(&model.Pipeline{ID: pipelineID}, nil)
	mockStore.On("StepLoad", mock.Anything, mock.Anything).
		Return(&model.Step{
			ID:         stepID,
			PipelineID: pipelineID,
			State:      model.StatusRunning,
		}, nil)

	done := make(chan struct{})
	router := gin.New()
	router.GET("/stream/logs/:repo_id/:pipeline/:step_id", func(c *gin.Context) {
		defer close(done)
		c.Set("store", mockStore)
		c.Set("repo", &model.Repo{ID: 1, FullName: "owner/repo"})
		LogStreamSSE(c)
	})

	ts = httptest.NewUnstartedServer(router)
	ts.Config.WriteTimeout = writeTimeout
	start(ts)
	t.Cleanup(ts.Close)

	return ts, logService, done
}

// TestLogStreamSSESurvivesWriteTimeout pins the rolling-deadline override for
// the log stream: a live LogStreamSSE stream keeps delivering log events well
// past the server-wide WriteTimeout, because the handler re-arms a rolling
// per-response write deadline (2*idlePingTime) before every write. It runs over
// both HTTP/1 and HTTP/2+TLS so the h2 responseWriter's SetWriteDeadline path
// is exercised too. Without the rolling deadline the server would tear the
// stream down at WriteTimeout and no log line would arrive after it — the
// assertion below would fail.
func TestLogStreamSSESurvivesWriteTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// The override proof needs the rolling deadline (2*idlePingTime) to outlast
	// the server-wide WriteTimeout on the FIRST arm, so the stream survives past
	// it even if the handler never gets to re-arm — otherwise survival would hinge
	// on re-arming within the window every time, which a scheduler stall under
	// load can miss (flaky). Keep 2*idlePingTime (200ms) > testWriteTimeout (80ms)
	// with margin. Mutates the shared idlePingTime global, so no t.Parallel().
	origPing := idlePingTime
	idlePingTime = 100 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	const testWriteTimeout = 80 * time.Millisecond
	const stepID int64 = 42

	transports := []struct {
		name  string
		start func(*httptest.Server)
	}{
		{"http1", func(ts *httptest.Server) { ts.Start() }},
		{"http2", func(ts *httptest.Server) { ts.EnableHTTP2 = true; ts.StartTLS() }},
	}

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			ts, logService, handlerDone := newSSELogServer(t, testWriteTimeout, tr.start)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/logs/1/1/42", nil)
			require.NoError(t, err)
			resp, err := ts.Client().Do(req)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, http.StatusOK, resp.StatusCode)

			// Emit log events continuously, past the WriteTimeout window.
			pubStop := make(chan struct{})
			go func() {
				ticker := time.NewTicker(30 * time.Millisecond)
				defer ticker.Stop()
				n := 0
				for {
					select {
					case <-pubStop:
						return
					case <-ticker.C:
						n++
						_ = logService.Write(context.Background(), stepID, []*model.LogEntry{
							{Line: n, Data: []byte(fmt.Sprintf("log line %d", n))},
						})
					}
				}
			}()
			t.Cleanup(func() { close(pubStop) })

			// Read the stream and confirm at least one line arrives strictly
			// after WriteTimeout has elapsed — proof the rolling deadline
			// overrode the server-wide one.
			start := time.Now()
			br := bufio.NewReader(resp.Body)
			readDeadline := time.Now().Add(2*testWriteTimeout + time.Second)
			var gotLate bool
			for time.Now().Before(readDeadline) {
				line, err := br.ReadString('\n')
				if err != nil {
					break
				}
				if strings.TrimSpace(line) == "" {
					continue
				}
				if time.Since(start) > testWriteTimeout {
					gotLate = true
					break
				}
			}
			require.True(t, gotLate,
				"log SSE stream stopped delivering before WriteTimeout elapsed; the rolling write deadline did not override the server-wide WriteTimeout")

			// Deterministically reap the handler goroutine before this subtest
			// returns. Canceling the request trips the handler's
			// requestCtx.Done() arm; waiting on handlerDone establishes a
			// happens-before with the parent's idlePingTime restore, so the
			// live handler can no longer race the restore's write of the
			// global. This is the fix for that race — not a reason to rerun
			// the test until it passes.
			cancel()
			select {
			case <-handlerDone:
			case <-time.After(2*testWriteTimeout + time.Second):
				t.Fatal("log SSE handler did not return after request cancel; goroutine leaked")
			}
		})
	}
}

// TestLogStreamSSEWedgeKill pins the return-on-write-error path for the log
// stream: when a client stops reading, the send buffer fills, and the blocked
// write is reclaimed by the rolling deadline (2*idlePingTime) — the handler
// observes the write error and returns instead of spinning forever.
// WriteTimeout is left 0 so the ONLY thing that can unblock the wedged write is
// the per-response deadline the fix arms; without it the handler would block
// indefinitely and this test would time out.
func TestLogStreamSSEWedgeKill(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Mutates the shared idlePingTime global, so this test must NOT run
	// t.Parallel().
	origPing := idlePingTime
	idlePingTime = 40 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	const stepID int64 = 42

	// WriteTimeout 0: no server-wide deadline. Only armSSEWriteDeadline can
	// reclaim the wedged write.
	ts, logService, handlerDone := newSSELogServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/logs/1/1/42", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The client deliberately never reads resp.Body. Flood large log entries so
	// the kernel send buffer fills and the next handler write blocks.
	pubStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		big := bytes.Repeat([]byte("x"), 256<<10)
		n := 0
		for {
			select {
			case <-pubStop:
				return
			case <-ticker.C:
				n++
				_ = logService.Write(context.Background(), stepID, []*model.LogEntry{
					{Line: n, Data: big},
				})
			}
		}
	}()
	t.Cleanup(func() { close(pubStop) })

	// The wedged write must be reclaimed by the rolling deadline and the
	// handler must return within 2*idlePingTime + slack.
	const slack = 3 * time.Second
	select {
	case <-handlerDone:
		// handler returned — the write error was surfaced and acted on.
	case <-time.After(2*idlePingTime + slack):
		t.Fatalf("log SSE handler did not return within 2*idlePingTime+slack (%s); "+
			"the wedged write was never reclaimed / the write error was not acted on", 2*idlePingTime+slack)
	}
}

// flushErrRecorder is an http.ResponseWriter that reports a flush failure —
// standing in for the real *http.response once its write deadline has passed.
// It implements ONLY FlushError (no plain Flush), so a controller that reaches
// it must surface the error.
type flushErrRecorder struct {
	http.ResponseWriter
	flushErr error
	flushes  int
}

func (w *flushErrRecorder) FlushError() error {
	w.flushes++
	return w.flushErr
}

// ginShapedWriter mimics the one property of gin's *responseWriter that matters
// here: it wraps another writer, exposes Unwrap, and implements the PLAIN
// http.Flusher (Flush() with no return value) — the arm that silently discards
// a flush error.
type ginShapedWriter struct {
	http.ResponseWriter
	flushes int
}

func (w *ginShapedWriter) Flush()                      { w.flushes++ }
func (w *ginShapedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// TestNewResponseControllerSurfacesFlushError pins the reason the helper
// unwraps at all, and it is the DISCRIMINATING test for the flush path.
//
// Why a unit test rather than an end-to-end wedge: to wedge a real socket you
// must first write enough bytes to fill it, and those writes are themselves an
// error path that reclaims the stream — which is exactly why
// TestLogStreamSSEWedgeKill (256KB floods) passes with or without the fix and
// cannot see a swallowed flush. Every end-to-end variant inherits that hole, so
// the flush behavior is pinned directly.
//
// Gin's *responseWriter implements the plain http.Flusher, so
// http.NewResponseController(ginWriter).Flush() matches its `case Flusher` arm
// and returns nil WITHOUT unwrapping to the underlying writer's FlushError —
// net/http/responsecontroller.go orders the Flusher case before the rwUnwrapper
// case. Every `if err := rc.Flush()` check upstack is then dead code, and a
// ping-only wedged stream — whose only socket write happens inside that flush —
// lingers up to a further idlePingTime instead of being reclaimed within the
// 2*idlePingTime the design promises.
func TestNewResponseControllerSurfacesFlushError(t *testing.T) {
	t.Parallel()

	newPair := func() (*flushErrRecorder, *ginShapedWriter) {
		underlying := &flushErrRecorder{
			ResponseWriter: httptest.NewRecorder(),
			flushErr:       os.ErrDeadlineExceeded,
		}
		return underlying, &ginShapedWriter{ResponseWriter: underlying}
	}

	// The fix: unwrapping past the gin-shaped writer reaches FlushError.
	fixedUnderlying, fixedWriter := newPair()
	fixedErr := newResponseController(fixedWriter).Flush()

	// The bug: building the controller straight on the gin-shaped writer lets
	// its plain Flush() win the type switch and swallow the error.
	buggyUnderlying, buggyWriter := newPair()
	buggyErr := http.NewResponseController(buggyWriter).Flush()

	require.ErrorIs(t, fixedErr, os.ErrDeadlineExceeded,
		"newResponseController must unwrap past gin so FlushError reaches the caller; "+
			"a nil here means every flush-error check in the SSE handlers is dead code")
	require.Equal(t, 1, fixedUnderlying.flushes, "the underlying FlushError must actually run")
	require.Zero(t, fixedWriter.flushes, "the gin-shaped Flush() must have been bypassed")

	require.NoError(t, buggyErr,
		"a plain http.Flusher cannot report an error — if this ever fails, the premise "+
			"behind the unwrap has changed and it may be removable")
	require.Zero(t, buggyUnderlying.flushes, "gin's Flush() must not have reached FlushError")

	// Assert the two paths DIFFER. Checking either arm alone would pass against
	// a broken controller; the difference is the actual claim.
	require.NotEqual(t, fixedErr == nil, buggyErr == nil,
		"unwrapped and non-unwrapped flushes must disagree — if they agree, this test "+
			"no longer discriminates and the fix is unproven")
}

// requireDeliveryThroughFinalInterval reads an SSE body for runFor and fails
// unless a line arrived inside the LAST 2*idlePingTime of that window.
//
// That final-interval check is what makes the caller a re-arm test rather than
// a first-arm test. The initial arm before the loop buys exactly one
// 2*idlePingTime of runway; a stream still delivering after several multiples
// of it can only be doing so because the handler re-armed inside the loop.
//
// Reading is bounded without a timer: when the deadline reclaims the response
// the server closes the connection and ReadString returns an error, so the
// loop ends on the event rather than on a guess.
func requireDeliveryThroughFinalInterval(t *testing.T, body io.Reader, runFor time.Duration) {
	t.Helper()

	start := time.Now()
	stopAfter := start.Add(runFor)
	finalInterval := stopAfter.Add(-2 * idlePingTime)

	var lastLineAt time.Time
	var lines int
	br := bufio.NewReader(body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			break
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		lastLineAt = time.Now()
		lines++
		if lastLineAt.After(stopAfter) {
			break
		}
	}

	require.NotZero(t, lines, "the stream delivered nothing at all")
	require.False(t, lastLineAt.Before(finalInterval),
		"stream stopped delivering %s into a %s run, with the last line %s before the "+
			"final %s window; the handler armed the write deadline once before the loop "+
			"and never re-armed inside it",
		lastLineAt.Sub(start), runFor, finalInterval.Sub(lastLineAt), 2*idlePingTime)
}

// TestEventStreamSSEReArmsThroughoutStream pins the re-arm contract that
// TestEventStreamSSESurvivesWriteTimeout is too coarse to see. That test sizes
// the rolling deadline (2*idlePingTime) longer than the server-wide
// WriteTimeout on purpose, so the single arm before the loop already carries
// the stream past the window and deleting every in-loop arm still passes it.
//
// Here the run is five times the rolling deadline and the assertion lands in
// the final interval, which no single arm can reach: only a handler that
// re-arms before each write keeps the response alive that long.
//
// HTTP/1 only. The transport does not change which arms run — it only changes
// the SetWriteDeadline implementation underneath, which the coarse tests
// already exercise over both.
func TestEventStreamSSEReArmsThroughoutStream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Mutates the shared idlePingTime global, so no t.Parallel().
	origPing := idlePingTime
	idlePingTime = 100 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	// Five rolling-deadline windows: long enough that a stream carried only by
	// the first arm is dead four windows before the assertion.
	runFor := 5 * 2 * idlePingTime

	// WriteTimeout 0 so the server-wide deadline cannot be what ends the
	// stream: the per-response arm is the only deadline in play.
	ts, broker, handlerDone := newSSEEventServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/events", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Publish faster than idlePingTime so the data arm, not the ping arm, is
	// what drives the loop — the ping arm would re-arm on its own schedule and
	// blur which call site is under test.
	pubStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		topic := map[string]struct{}{pubsub.PublicTopic: {}}
		for {
			select {
			case <-pubStop:
				return
			case <-ticker.C:
				_ = broker.Publish(context.Background(), topic,
					pubsub.Message{Data: []byte(`{"pipeline":1}`)})
			}
		}
	}()
	t.Cleanup(func() { close(pubStop) })

	requireDeliveryThroughFinalInterval(t, resp.Body, runFor)

	// Reap the handler before the parent restores idlePingTime, so the live
	// handler's read of the global cannot race that write.
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE handler did not return after request cancel; goroutine leaked")
	}
}

// TestLogStreamSSEReArmsThroughoutStream is TestEventStreamSSEReArmsThroughoutStream
// for the log stream: same reasoning, same mutant, the other handler's loop.
func TestLogStreamSSEReArmsThroughoutStream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origPing := idlePingTime
	idlePingTime = 100 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	const stepID int64 = 42
	runFor := 5 * 2 * idlePingTime

	ts, logService, handlerDone := newSSELogServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/logs/1/1/42", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	pubStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		n := 0
		for {
			select {
			case <-pubStop:
				return
			case <-ticker.C:
				n++
				_ = logService.Write(context.Background(), stepID, []*model.LogEntry{
					{Line: n, Data: []byte(fmt.Sprintf("log line %d", n))},
				})
			}
		}
	}()
	t.Cleanup(func() { close(pubStop) })

	requireDeliveryThroughFinalInterval(t, resp.Body, runFor)

	cancel()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("log SSE handler did not return after request cancel; goroutine leaked")
	}
}

// TestLogStreamSSEEOFMarkerReachesHealthyClient pins that the eof marker
// actually reaches a client that never stopped reading.
//
// The eof write lives in the ctx.Done() arm, and every other write site arms
// the deadline immediately before writing. If that arm alone is missing, the
// last arm in force can be arbitrarily old by the time the logs end, because
// two things reset the ping countdown without re-arming: any select arm firing
// restarts time.After(idlePingTime), and the replay path re-arms only inside
// `id > last`. So a reconnect replaying more than 2*idlePingTime of backlog
// runs the loop hard with an already-expired deadline, and the eof write fails
// against a perfectly healthy socket. The client's EventSource then reconnects
// forever waiting for an end that was written and dropped.
//
// The reproduction is exactly that shape: Last-Event-ID far ahead of the live
// ids so the loop churns through the replay path, a burst longer than the
// rolling deadline, then the log service closes to trigger eof. WriteTimeout is
// 0 so nothing but the per-response arm governs the write.
func TestLogStreamSSEEOFMarkerReachesHealthyClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origPing := idlePingTime
	idlePingTime = 150 * time.Millisecond
	t.Cleanup(func() { idlePingTime = origPing })

	const stepID int64 = 42
	// Three rolling-deadline windows of replay: the deadline armed before the
	// loop is long expired by the time the logs end.
	const replayFor = 900 * time.Millisecond

	ts, logService, handlerDone := newSSELogServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/stream/logs/1/1/42", nil)
	require.NoError(t, err)
	// Far ahead of any id this stream will reach, so `id > last` is never true
	// and the loop drives without passing a write site.
	req.Header.Set("Last-Event-ID", "100000")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// A healthy client: it reads continuously and never stops, so nothing about
	// the peer can explain a failed write.
	gotEOF := make(chan struct{})
	go func() {
		br := bufio.NewReader(resp.Body)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "event: eof") {
				close(gotEOF)
				return
			}
		}
	}()

	burstDone := time.After(replayFor)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for n := 0; ; {
		stop := false
		select {
		case <-burstDone:
			stop = true
		case <-ticker.C:
			n++
			require.NoError(t, logService.Write(context.Background(), stepID,
				[]*model.LogEntry{{Line: n, Data: []byte(fmt.Sprintf("replayed line %d", n))}}))
		}
		if stop {
			break
		}
	}

	// Closing the stream ends Tail, which cancels the handler's context with
	// context.Canceled — the eof arm.
	require.NoError(t, logService.Close(context.Background(), stepID))

	select {
	case <-gotEOF:
	case <-handlerDone:
		// The handler returned without the marker ever landing: the eof write
		// or its flush failed on an expired deadline.
		select {
		case <-gotEOF:
		case <-time.After(time.Second):
			t.Fatal("log stream ended without delivering the eof marker to a client that " +
				"never stopped reading; the eof write ran against a write deadline that " +
				"expired during replay and was never re-armed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("neither the eof marker nor the handler's return arrived")
	}
}

// A stream carrying only keep-alive pings is the case the absolute ceiling
// exists for, and the one the rolling deadline cannot reach: a 9-byte ping
// never fills a socket send buffer, so its write never blocks and no write
// deadline it arms can ever expire. Left to the rolling deadline alone such a
// stream is held for as long as the peer keeps the connection open without
// reading — measured at 75x the interval the spec promises, and unbounded in
// principle.
//
// These pin the timer specifically. The clamp in armSSEWriteDeadline is covered
// by TestArmSSEWriteDeadlineClampsToTheCeiling, but the clamp is not what ends
// the stream: with the timer neutered and the clamp intact, a ping-only stream
// is not reclaimed at all. Only an end-to-end test distinguishes them.
func TestEventStreamSSEPingOnlyStreamEndsAtTheCeiling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	shortenStreamBoundsForTest(t)

	// WriteTimeout 0: with no server-wide deadline in play, the ceiling is the
	// only thing that can end this handler.
	ts, _, handlerDone := newSSEEventServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	resp := getAndReadFirstPing(t, ts.URL+"/stream/events")
	// Stop reading without closing, and publish nothing: pings only, to a peer
	// that never drains them.
	t.Cleanup(func() { _ = resp.Body.Close() })

	select {
	case <-handlerDone:
	case <-time.After(streamMaxDuration + 5*time.Second):
		t.Fatal("ping-only stream outlived its absolute ceiling; the handler was never reclaimed")
	}
}

func TestLogStreamSSEPingOnlyStreamEndsAtTheCeiling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	shortenStreamBoundsForTest(t)

	ts, _, handlerDone := newSSELogServer(t, 0, func(ts *httptest.Server) { ts.Start() })

	resp := getAndReadFirstPing(t, ts.URL+"/stream/logs/1/1/42")
	t.Cleanup(func() { _ = resp.Body.Close() })

	select {
	case <-handlerDone:
	case <-time.After(streamMaxDuration + 5*time.Second):
		t.Fatal("ping-only log stream outlived its absolute ceiling; the handler was never reclaimed")
	}
}

// shortenStreamBoundsForTest scales the ping interval and the ceiling down so a
// ceiling that is an hour in production is observable in a test. Mutating the
// package globals means these tests must not run with t.Parallel(), per the
// rule documented on idlePingTime.
func shortenStreamBoundsForTest(t *testing.T) {
	t.Helper()
	origPing, origMax := idlePingTime, streamMaxDuration
	idlePingTime, streamMaxDuration = 50*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { idlePingTime, streamMaxDuration = origPing, origMax })
}

// getAndReadFirstPing opens the stream and consumes its opening ping, so the
// caller knows the handler has reached its loop before it stops reading.
func getAndReadFirstPing(t *testing.T, url string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	buf := make([]byte, len(": ping\n\n"))
	_, err = io.ReadFull(resp.Body, buf)
	require.NoError(t, err, "the stream should open with a ping")
	require.Equal(t, ": ping\n\n", string(buf))

	return resp
}
