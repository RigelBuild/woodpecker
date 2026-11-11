// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/rpc/proto"
)

type waitRetryServer struct {
	proto.UnimplementedWoodpeckerServer

	firstWaitStarted  chan struct{}
	firstWaitDuration time.Duration
	persistentError   bool
	calls             atomic.Int32
}

func (s *waitRetryServer) Wait(ctx context.Context, _ *proto.WaitRequest) (*proto.WaitResponse, error) {
	if s.calls.Add(1) == 1 {
		timer := time.NewTimer(s.firstWaitDuration)
		defer timer.Stop()
		close(s.firstWaitStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, status.Error(codes.Unavailable, "server restarted")
		}
	}
	if s.persistentError {
		return nil, status.Error(codes.Unavailable, "server unavailable")
	}
	return &proto.WaitResponse{Canceled: false}, nil
}

func newWaitRetryClient(t *testing.T, service proto.WoodpeckerServer, timeout time.Duration) *client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	proto.RegisterWoodpeckerServer(server, service)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("server.Serve() = %v, want nil or grpc.ErrServerStopped", err)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close(): %v", err)
		}
		if err := listener.Close(); err != nil {
			t.Errorf("listener.Close(): %v", err)
		}
	})

	return &client{
		client:                 proto.NewWoodpeckerClient(conn),
		conn:                   conn,
		connectionRetryTimeout: timeout,
	}
}

type extendNotFoundServer struct {
	proto.UnimplementedWoodpeckerServer
	calls atomic.Int32
}

func (s *extendNotFoundServer) Extend(context.Context, *proto.ExtendRequest) (*proto.Empty, error) {
	s.calls.Add(1)
	return nil, status.Error(codes.NotFound, "queue: task not found")
}

func TestClientExtendNotFoundIsNotRetried(t *testing.T) {
	service := &extendNotFoundServer{}
	c := newWaitRetryClient(t, service, time.Second)

	err := c.Extend(t.Context(), "workflow-id")
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, int32(1), service.calls.Load())
}

func TestWaitRetryTimeoutStartsAfterLongPollFailure(t *testing.T) {
	tests := []struct {
		name            string
		persistentError bool
	}{
		{name: "recovers after one unavailable response"},
		{name: "persistent unavailable response exhausts timeout", persistentError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				service := &waitRetryServer{
					firstWaitStarted:  make(chan struct{}),
					firstWaitDuration: 5 * time.Minute,
					persistentError:   tt.persistentError,
				}
				const retryTimeout = 2 * time.Minute
				c := newWaitRetryClient(t, service, retryTimeout)
				type result struct {
					canceled bool
					err      error
				}
				resultCh := make(chan result, 1)
				go func() {
					canceled, err := c.Wait(t.Context(), "workflow-id")
					resultCh <- result{canceled: canceled, err: err}
				}()

				<-service.firstWaitStarted
				synctest.Wait()
				got := <-resultCh
				if tt.persistentError {
					require.Error(t, got.err)
					assert.ErrorIs(t, got.err, backoff.ErrMaxElapsedTime)
					assert.Greater(t, service.calls.Load(), int32(1))
					return
				}

				require.NoError(t, got.err)
				assert.False(t, got.canceled)
				assert.Equal(t, int32(2), service.calls.Load())
			})
		})
	}
}

type doneCaptureServer struct {
	proto.UnimplementedWoodpeckerServer
	requests chan *proto.DoneRequest
}

func (s *doneCaptureServer) Done(_ context.Context, req *proto.DoneRequest) (*proto.Empty, error) {
	s.requests <- req
	return &proto.Empty{}, nil
}

func TestClientDoneSendsAgentShutdown(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	capture := &doneCaptureServer{requests: make(chan *proto.DoneRequest, 1)}
	proto.RegisterWoodpeckerServer(server, capture)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := NewGrpcClient(t.Context(), conn, SetConnectionRetryTimeout(time.Second))
	require.NoError(t, client.Done(t.Context(), "workflow-id", rpc.WorkflowState{AgentShutdown: true, Evicted: true}))
	select {
	case req := <-capture.requests:
		assert.True(t, req.GetState().GetAgentShutdown())
		assert.True(t, req.GetState().GetEvicted())
	case <-time.After(time.Second):
		t.Fatal("Done request was not received")
	}
}

func TestSetConnectionRetryTimeout(t *testing.T) {
	tc := []struct {
		name    string
		timeout time.Duration
	}{
		{"finite", 5 * time.Minute},
		{"zero means infinite", 0},
	}

	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			cl := &client{}
			SetConnectionRetryTimeout(c.timeout)(cl)
			assert.Equal(t, c.timeout, cl.connectionRetryTimeout)
		})
	}
}

func TestIsConnected(t *testing.T) {
	cl := &client{conn: newTestConn(t)}
	defer cl.conn.Close()

	t.Run("idle connection reports connected", func(t *testing.T) {
		assert.True(t, cl.IsConnected())
	})

	t.Run("closed connection reports not connected", func(t *testing.T) {
		assert.NoError(t, cl.conn.Close())
		assert.False(t, cl.IsConnected())
	})
}

func TestClassifyRPCErrUnauthenticatedIsRetryable(t *testing.T) {
	t.Parallel()

	err := status.Error(codes.Unauthenticated, "expired token")
	classified := classifyRPCErr(t.Context(), err)

	assert.Equal(t, codes.Unauthenticated, status.Code(classified))
	assert.False(t, errors.Is(classified, backoff.ErrPermanent))
}

func TestRetryRPCUnauthenticatedHonorsFiniteTimeout(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	c := &client{connectionRetryTimeout: time.Nanosecond}
	var attempts int

	_, err := retryRPC(ctx, c, "test", func() (struct{}, error) {
		attempts++
		return struct{}{}, classifyRPCErr(ctx, status.Error(codes.Unauthenticated, "expired token"))
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, backoff.ErrMaxElapsedTime)
	assert.Equal(t, 1, attempts)
}

func TestRetryRPCUnauthenticatedRetriesUntilContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	c := &client{connectionRetryTimeout: 0}
	var attempts int

	_, err := retryRPC(ctx, c, "test", func() (struct{}, error) {
		attempts++
		if attempts == 3 {
			cancel(nil)
		}
		return struct{}{}, classifyRPCErr(ctx, status.Error(codes.Unauthenticated, "expired token"))
	})

	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestRetryLongPollRPCMatchesRetryRPCOutcomes(t *testing.T) {
	tests := []struct {
		name            string
		cancelContext   bool
		firstError      func(context.Context) error
		timeout         time.Duration
		wantErr         error
		wantPermanent   bool
		wantMinAttempts int32
	}{
		{
			name: "server cancellation is returned",
			firstError: func(ctx context.Context) error {
				return classifyRPCErr(ctx, status.Error(codes.Canceled, "server canceled"))
			},
			wantPermanent:   true,
			wantMinAttempts: 1,
		},
		{
			name:          "caller cancellation returns zero result",
			cancelContext: true,
			firstError: func(ctx context.Context) error {
				return classifyRPCErr(ctx, status.Error(codes.Unavailable, "server unavailable"))
			},
			wantMinAttempts: 1,
		},
		{
			name: "connection loss exhausts retry timeout",
			firstError: func(context.Context) error {
				return errNotConnected
			},
			timeout:         time.Second,
			wantErr:         ErrConnectionLost,
			wantMinAttempts: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				run := func(useLongPoll bool) (struct{}, int32, error) {
					ctx := t.Context()
					if tt.cancelContext {
						var cancel context.CancelCauseFunc
						ctx, cancel = context.WithCancelCause(ctx)
						cancel(context.Canceled)
					}
					c := &client{connectionRetryTimeout: tt.timeout}
					var callCount atomic.Int32
					op := func() (struct{}, error) {
						callCount.Add(1)
						return struct{}{}, tt.firstError(ctx)
					}
					var (
						result struct{}
						err    error
					)
					if useLongPoll {
						result, err = retryLongPollRPC(ctx, c, "test", op)
					} else {
						result, err = retryRPC(ctx, c, "test", op)
					}
					return result, callCount.Load(), err
				}

				longPollResult, longPollAttempts, longPollErr := run(true)
				retryResult, retryAttempts, retryErr := run(false)
				assert.Equal(t, retryResult, longPollResult)
				assert.GreaterOrEqual(t, longPollAttempts, tt.wantMinAttempts)
				assert.GreaterOrEqual(t, retryAttempts, tt.wantMinAttempts)
				if tt.wantErr != nil {
					assert.ErrorIs(t, longPollErr, tt.wantErr)
					assert.ErrorIs(t, retryErr, tt.wantErr)
					return
				}
				if tt.wantPermanent {
					assert.ErrorIs(t, longPollErr, backoff.ErrPermanent)
					assert.ErrorIs(t, retryErr, backoff.ErrPermanent)
					assert.Equal(t, codes.Canceled, status.Code(backoff.AsRetryError(longPollErr).LastErr))
					assert.Equal(t, codes.Canceled, status.Code(backoff.AsRetryError(retryErr).LastErr))
					return
				}
				assert.NoError(t, longPollErr)
				assert.NoError(t, retryErr)
			})
		})
	}
}

type logRecorderClient struct {
	proto.WoodpeckerClient

	mu      sync.Mutex
	batches [][]*proto.LogEntry
}

func (l *logRecorderClient) Log(_ context.Context, in *proto.LogRequest, _ ...grpc.CallOption) (*proto.Empty, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batches = append(l.batches, in.GetLogEntries())
	return &proto.Empty{}, nil
}

// sent returns the number of Log calls and the total number of entries sent.
func (l *logRecorderClient) sent() (batches, entries int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, b := range l.batches {
		entries += len(b)
	}
	return len(l.batches), entries
}

func TestProcessLogsFlushesContinuousStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		recorder := &logRecorderClient{}
		c := &client{
			client: recorder,
			logs:   make(chan *proto.LogEntry, 10),
		}
		go c.processLogs(ctx)

		// Emit a small log line more often than maxLogFlushPeriod, so the stream
		// never goes quiet long enough for an idle timeout to fire.
		const interval = maxLogFlushPeriod / 4
		const total = 20 // 5 * maxLogFlushPeriod of continuous output
		for i := range total {
			c.EnqueueLog(&rpc.LogEntry{StepUUID: "step", Line: i, Data: []byte("line")})
			time.Sleep(interval)
			synctest.Wait()
		}

		// While output is still streaming, logs must have been flushed
		// periodically instead of being held back until the stream goes quiet.
		batches, entries := recorder.sent()
		assert.GreaterOrEqual(t, batches, 4)
		assert.Greater(t, entries, total-4)

		// Once the stream is idle everything must be delivered.
		time.Sleep(maxLogFlushPeriod)
		synctest.Wait()
		_, entries = recorder.sent()
		assert.Equal(t, total, entries)

		cancel(nil)
		synctest.Wait()
	})
}
