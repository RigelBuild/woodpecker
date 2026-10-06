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
	"testing"
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
	require.NoError(t, client.Done(t.Context(), "workflow-id", rpc.WorkflowState{AgentShutdown: true}))
	select {
	case req := <-capture.requests:
		assert.True(t, req.GetState().GetAgentShutdown())
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
	classified := classifyRPCErr(context.Background(), err)

	assert.Equal(t, codes.Unauthenticated, status.Code(classified))
	assert.False(t, errors.Is(classified, backoff.ErrPermanent))
}

func TestRetryRPCUnauthenticatedHonorsFiniteTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
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

	ctx, cancel := context.WithCancelCause(context.Background())
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
