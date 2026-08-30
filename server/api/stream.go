// Copyright 2022 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

const (
	// How many batches of logs to keep for each client before starting to
	// drop them if the client is not consuming them faster than they arrive.
	maxQueuedBatchesPerClient int = 30
)

// idlePingTime is the interval between keep-alive pings on an SSE stream. It is
// a var, not a const, so tests can shorten it to exercise the rolling
// SetWriteDeadline path without real-time waits. Tests that mutate it MUST NOT
// run with t.Parallel(): it is a shared package-level global read by the SSE
// handlers, so a parallel mutator races the handler's read under `go test -race`.
var idlePingTime = time.Second * 30

// streamMaxDuration is the absolute ceiling on a single SSE response, measured
// from the start of the request. It backstops the rolling write deadline for a
// stream whose peer stops reading without closing and which carries only
// keep-alive pings — see armSSEWriteDeadline. Reaching it ends the response;
// the client's EventSource reconnects.
//
// A var for the same test reason as idlePingTime, and under the same rule.
var streamMaxDuration = time.Hour

// streamCeilingJitterDivisor bounds the ceiling jitter to streamMaxDuration
// divided by this factor (i.e. up to a tenth of the ceiling). See streamCeiling.
const streamCeilingJitterDivisor = 10

// streamCeiling returns the absolute per-connection deadline for one SSE
// response: now + streamMaxDuration, less a bounded random jitter in
// [0, streamMaxDuration/10]. The jitter de-synchronizes a cohort of clients
// that connected together (e.g. every dashboard tab reconnecting at once after
// a server restart), so their ceilings expire spread out rather than re-forming
// a thundering herd every streamMaxDuration. This is load spreading rather than
// a security boundary, so math/rand/v2 is sufficient. Computed once per handler
// invocation.
func streamCeiling() time.Time {
	jitter := rand.N(streamMaxDuration / streamCeilingJitterDivisor) //nolint:gosec // load spreading, not security
	return time.Now().Add(streamMaxDuration - jitter)
}

// EventStreamSSE
//
//	@Summary		Stream events like pipeline updates
//	@Description	With quic and http2 support
//	@Router			/stream/events [get]
//	@Produce		plain
//	@Success		200
//	@Tags			Events
func EventStreamSSE(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-store")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	rw := c.Writer

	rc := newResponseController(rw)
	// One warn per response if this writer cannot take a deadline; the arms
	// below run per write.
	var deadlineWarnOnce sync.Once
	streamUntil := streamCeiling()

	// ping the client. Arm before the write, as every write site does — the
	// first flush is also what reports a writer that cannot stream at all,
	// since rc.Flush surfaces ErrNotSupported where a Flusher type-assertion
	// on the gin writer would not (gin always implements Flush; the wrapped
	// writer is the one that matters).
	armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
	if _, err := io.WriteString(rw, ": ping\n\n"); err != nil {
		return
	}
	if err := rc.Flush(); err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			c.String(http.StatusInternalServerError, "Streaming not supported")
		}
		return
	}

	log.Debug().Msg("user feed: connection opened")

	user := session.User(c)
	subTopics := make(map[string]struct{})
	// subscribe to all public state changes
	subTopics[pubsub.PublicTopic] = struct{}{}
	// subscribe to all private state changes or repos the user owns
	if user != nil {
		repos, _ := store.FromContext(c).RepoList(user, false, true, nil)
		for _, r := range repos {
			subTopics[pubsub.GetRepoTopic(r)] = struct{}{}
		}
	}

	eventChan := make(chan []byte, 10)
	ctx, cancel := context.WithCancelCause(
		context.Background(),
	)
	requestCtx := c.Request.Context()

	defer func() {
		cancel(nil)
		log.Debug().Msg("user feed: connection closed")
	}()

	// Captured once: this goroutine outlives the handler, and reading the global
	// from inside it would read it after a test's cleanup has reset it.
	scheduler := server.Config.Services.Scheduler
	go func() {
		err := scheduler.Subscribe(ctx, subTopics,
			func(m pubsub.Message) {
				select {
				case <-ctx.Done():
				case eventChan <- m.Data:
				}
			})
		cancel(err)
	}()

	streamExpiry := time.NewTimer(time.Until(streamUntil))
	defer streamExpiry.Stop()

	for {
		select {
		case <-requestCtx.Done():
			return
		case <-ctx.Done():
			return
		case <-streamExpiry.C:
			// The absolute ceiling: a stream that reached it has run its full
			// allowance without the peer closing. Ending the response releases
			// the handler and subscription; a live client reconnects.
			log.Debug().Msg("user feed: stream reached its maximum duration")
			return
		case <-time.After(idlePingTime):
			armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
			if _, err := io.WriteString(rw, ": ping\n\n"); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		case buf, ok := <-eventChan:
			if ok {
				armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
				if _, err := io.WriteString(rw, "data: "); err != nil {
					return
				}
				if _, err := rw.Write(buf); err != nil {
					return
				}
				if _, err := io.WriteString(rw, "\n\n"); err != nil {
					return
				}
				if err := rc.Flush(); err != nil {
					return
				}
			}
		}
	}
}

// LogStreamSSE
//
//	@Summary	Stream logs of a pipeline step
//	@Router		/stream/logs/{repo_id}/{pipeline}/{step_id} [get]
//	@Produce	plain
//	@Success	200
//	@Tags		Pipeline logs
//	@Param		repo_id		path	int	true	"the repository id"
//	@Param		pipeline	path	int	true	"the number of the pipeline"
//	@Param		step_id		path	int	true	"the step id"
func LogStreamSSE(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	rw := c.Writer

	rc := newResponseController(rw)
	// One warn per response if this writer cannot take a deadline; the arms
	// below run per write.
	var deadlineWarnOnce sync.Once
	streamUntil := streamCeiling()

	// Arm before the write, as every write site does. The first flush doubles
	// as the streaming-capability check: rc.Flush surfaces ErrNotSupported from
	// the writer that actually carries the flush, which a Flusher assertion on
	// the gin wrapper would not.
	armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
	if _, err := io.WriteString(rw, ": ping\n\n"); err != nil {
		return
	}
	if err := rc.Flush(); err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			c.String(http.StatusInternalServerError, "Streaming not supported")
		}
		return
	}

	_store := store.FromContext(c)
	repo := session.Repo(c)

	pipeline, err := strconv.ParseInt(c.Param("pipeline"), 10, 64)
	if err != nil {
		log.Debug().Err(err).Msg("pipeline number invalid")
		logWriteStringErr(io.WriteString(rw, "event: error\ndata: pipeline number invalid\n\n"))
		return
	}
	pl, err := _store.GetPipelineNumber(repo, pipeline)
	if err != nil {
		log.Debug().Err(err).Msg("stream cannot get pipeline number")
		logWriteStringErr(io.WriteString(rw, "event: error\ndata: pipeline not found\n\n"))
		return
	}

	stepID, err := strconv.ParseInt(c.Param("step_id"), 10, 64)
	if err != nil {
		log.Debug().Err(err).Msg("step id invalid")
		logWriteStringErr(io.WriteString(rw, "event: error\ndata: step id invalid\n\n"))
		return
	}
	step, err := _store.StepLoad(pl.ID, stepID)
	if err != nil {
		log.Debug().Err(err).Msg("stream cannot get step number")
		logWriteStringErr(io.WriteString(rw, "event: error\ndata: process not found\n\n"))
		return
	}

	if step.State != model.StatusPending && step.State != model.StatusRunning {
		log.Debug().Msg("step not running (anymore).")
		logWriteStringErr(io.WriteString(rw, "event: error\ndata: step not running (anymore)\n\n"))
		return
	}

	logChan := make(chan []byte, 10)
	ctx, cancel := context.WithCancelCause(
		context.Background(),
	)
	requestCtx := c.Request.Context()

	log.Debug().Msg("log stream: connection opened")

	defer func() {
		cancel(nil)
		log.Debug().Msg("log stream: connection closed")
	}()

	// Captured once: the tail goroutine below outlives this handler, and reading
	// the global from there would read it after a test's cleanup has reset it.
	logService := server.Config.Services.Logs
	err = logService.Open(ctx, step.ID)
	if err != nil {
		log.Error().Err(err).Msg("log stream: open failed")
		logWriteStringErr(io.WriteString(rw, "event: error\ndata: can't open stream\n\n"))
		return
	}

	go func() {
		batches := make(logging.LogChan, maxQueuedBatchesPerClient)

		var innerDone sync.WaitGroup
		innerDone.Add(1)
		go func() {
			defer innerDone.Done()
			for entries := range batches {
				for _, entry := range entries {
					if ee, err := json.Marshal(entry); err == nil {
						select {
						case <-ctx.Done():
							return
						case logChan <- ee:
						}
					} else {
						log.Error().Err(err).Msg("unable to serialize log entry")
					}
				}
			}
		}()

		err := logService.Tail(ctx, step.ID, batches)
		if err != nil {
			log.Error().Err(err).Msg("tail of logs failed")
		}

		close(batches)
		innerDone.Wait()
		cancel(err)
	}()

	id := 1
	last, _ := strconv.Atoi(
		c.Request.Header.Get("Last-Event-ID"),
	)
	if last != 0 {
		log.Debug().Msgf("log stream: reconnect: last-event-id: %d", last)
	}

	streamExpiry := time.NewTimer(time.Until(streamUntil))
	defer streamExpiry.Stop()

	for {
		select {
		case <-ctx.Done(): // Monitor if the "tail" context is canceled.
			// Return UNCONDITIONALLY: the tail context is done, so this stream
			// is over regardless of cause. Only the eof marker is conditional —
			// an ordinary end-of-logs cancellation (context.Canceled) tells the
			// client to stop; any other cause (e.g. a Tail error like ErrNotFound
			// on a race) has no marker to send. Falling through without returning
			// would re-select an already-closed ctx.Done() and spin at 100% CPU
			// until the ceiling.
			if err := context.Cause(ctx); errors.Is(err, context.Canceled) {
				log.Debug().Msg("log stream: eof")
				// Arm before this write like every other write site. The last
				// arm can be arbitrarily stale by now: any select arm firing
				// restarts the ping countdown, and the replay path re-arms only
				// inside `id > last`, so a reconnect that replays for longer
				// than the deadline leaves it already expired. Without a fresh
				// arm a healthy, still-reading client silently never receives
				// the eof marker and its EventSource reconnects forever.
				armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
				logWriteStringErr(io.WriteString(rw, "event: eof\ndata: eof\n\n"))
				// Best-effort: the stream is ending on this return either way,
				// so a failed final flush only means the peer never saw the
				// eof marker. Logged rather than discarded silently.
				if err := rc.Flush(); err != nil {
					log.Debug().Err(err).Msg("log stream: flushing eof marker")
				}
			}
			return
		case <-requestCtx.Done(): // Monitor the request context for cancellation when the client has gone away.
			log.Debug().Msg("log stream: closed, client has gone away")
			return
		case <-streamExpiry.C:
			// The absolute ceiling — see armSSEWriteDeadline. A log stream that
			// reaches it ends without an eof marker, because the logs have not
			// ended; the client reconnects with Last-Event-ID and resumes.
			log.Debug().Msg("log stream: reached its maximum duration")
			return
		case <-time.After(idlePingTime):
			armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
			if _, err := io.WriteString(rw, ": ping\n\n"); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		case buf, ok := <-logChan:
			if ok {
				if id > last {
					armSSEWriteDeadline(rc, streamUntil, &deadlineWarnOnce)
					if _, err := io.WriteString(rw, "id: "+strconv.Itoa(id)); err != nil {
						return
					}
					if _, err := io.WriteString(rw, "\n"); err != nil {
						return
					}
					if _, err := io.WriteString(rw, "data: "); err != nil {
						return
					}
					if _, err := rw.Write(buf); err != nil {
						return
					}
					if _, err := io.WriteString(rw, "\n\n"); err != nil {
						return
					}
					if err := rc.Flush(); err != nil {
						return
					}
				}
				id++
			}
		}
	}
}

func logWriteStringErr(_ int, err error) {
	if err != nil {
		log.Error().Err(err).Caller(1).Msg("fail to write string")
	}
}

// armSSEWriteDeadline arms the rolling per-response write deadline on an SSE
// stream, refreshed before each write so a healthy stream outlives the
// server-wide WriteTimeout while a peer that stops reading still trips the
// deadline and unblocks the handler.
//
// A ping-only stream is reclaimed by the ceiling TIMER, not by this arm: each
// handler builds a streamExpiry timer for streamMaxDuration and returns on its
// arm (EventStreamSSE, LogStreamSSE). That timer is what bounds the idle
// /api/stream/events case every open browser tab holds — the rolling deadline
// alone would not, because it only fires when a write actually blocks, and a
// 9-byte keep-alive ping never fills a socket send buffer (roughly 277k pings,
// some 96 days at the default interval, before one would).
//
// The clamp below is narrower: it keeps an arm from landing in the past as the
// response approaches streamMaxDuration, so a write racing the expiry timer
// ends the stream on the timer's clean return rather than on a write error.
//
// A healthy stream is unaffected until the ceiling: it re-arms every
// idlePingTime against a 2*idlePingTime deadline, so it keeps a full
// idlePingTime of slack. On reaching the ceiling the handler returns and the
// client's EventSource reconnects, which is the normal SSE lifecycle.
//
// The controller passed here must be one built by newResponseController: the
// ping's only socket write happens inside the flush whose error gin would
// otherwise swallow, so a ping-only stream is the case that most needs the
// unwrap.
func armSSEWriteDeadline(rc *http.ResponseController, until time.Time, warnOnce *sync.Once) {
	// A healthy stream re-arms every idlePingTime against a deadline two ping
	// intervals out, so it always keeps a full idlePingTime of slack.
	const pingSlackIntervals = 2
	d := pingSlackIntervals * idlePingTime
	// Clamp to the ceiling, but never to a deadline in the past. A write can
	// race the expiry timer and find no time remaining; arming that verbatim
	// would fail the write and end the stream on an error rather than on the
	// clean return the expiry arm makes. The floor lets the in-flight write
	// finish — the timer ends the stream on the next loop either way.
	if remaining := time.Until(until); remaining < d {
		d = max(remaining, idlePingTime)
	}
	extendWriteDeadline(rc, d, warnOnce)
}
