<<<<<<< conflict 1 of 1
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
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

package addon

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func newStatusRPCClient(t *testing.T, impl *forge_mocks.MockForge) *RPC {
	t.Helper()

	server := rpc.NewServer()
	require.NoError(t, server.RegisterName("Plugin", &RPCServer{Impl: impl}))

	clientConn, serverConn := net.Pipe()
	client := rpc.NewClient(clientConn)
	go server.ServeConn(serverConn)
	t.Cleanup(func() {
		if err := client.Close(); err != nil && !errors.Is(err, rpc.ErrShutdown) {
			t.Errorf("close RPC client: %v", err)
		}
		if err := serverConn.Close(); err != nil {
			t.Errorf("close RPC server connection: %v", err)
		}
	})

	return &RPC{client: client}
}

func TestRPCStatusDeadline(t *testing.T) {
	t.Run("plugin status receives caller deadline", func(t *testing.T) {
		impl := forge_mocks.NewMockForge(t)
		remoteContext := make(chan context.Context, 1)
		remoteResult := make(chan error, 1)
		client := newStatusRPCClient(t, impl)
		impl.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, _ *model.Workflow) error {
				remoteContext <- ctx
				<-ctx.Done()
				remoteResult <- ctx.Err()
				return ctx.Err()
			})
		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()
		callerDeadline, ok := ctx.Deadline()
		require.True(t, ok)

		result := make(chan error, 1)
		go func() {
			result <- client.Status(ctx, testUser(), testRepo(), &model.Pipeline{}, &model.Workflow{})
		}()

		select {
		case err := <-result:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(time.Second):
			t.Fatal("Status did not return at the caller deadline")
		}

		select {
		case got := <-remoteContext:
			remoteDeadline, remoteHasDeadline := got.Deadline()
			require.True(t, remoteHasDeadline)
			require.True(t, callerDeadline.Equal(remoteDeadline))
		case <-time.After(time.Second):
			t.Fatal("plugin Status was not called")
		}

		select {
		case err := <-remoteResult:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(time.Second):
			t.Fatal("plugin Status context did not expire")
		}
	})

	t.Run("caller cancellation returns while plugin remains blocked until deadline", func(t *testing.T) {
		impl := forge_mocks.NewMockForge(t)
		remoteContext := make(chan context.Context, 1)
		remoteResult := make(chan error, 1)
		client := newStatusRPCClient(t, impl)
		impl.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, _ *model.Workflow) error {
				remoteContext <- ctx
				<-ctx.Done()
				remoteResult <- ctx.Err()
				return ctx.Err()
			})

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			result <- client.Status(ctx, testUser(), testRepo(), &model.Pipeline{}, &model.Workflow{})
		}()

		var pluginContext context.Context
		select {
		case pluginContext = <-remoteContext:
		case <-time.After(time.Second):
			t.Fatal("plugin Status was not called")
		}
		pluginDeadline, ok := pluginContext.Deadline()
		require.True(t, ok)

		cancel()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(250 * time.Millisecond):
			t.Fatal("Status did not return when caller context was canceled")
		}
		require.NoError(t, pluginContext.Err())

		remaining := max(time.Until(pluginDeadline), 0)
		select {
		case err := <-remoteResult:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(remaining + 250*time.Millisecond):
			t.Fatal("plugin Status outlived its sent deadline")
		}
	})

	for _, sentinel := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run("plugin "+sentinel.Error()+" keeps its sentinel when the caller context is still live", func(t *testing.T) {
			impl := forge_mocks.NewMockForge(t)
			client := newStatusRPCClient(t, impl)
			impl.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(sentinel)

			err := client.Status(t.Context(), testUser(), testRepo(), &model.Pipeline{}, &model.Workflow{})
			require.ErrorIs(t, err, sentinel)
		})
	}

	t.Run("other plugin errors pass through unchanged", func(t *testing.T) {
		impl := forge_mocks.NewMockForge(t)
		client := newStatusRPCClient(t, impl)
		impl.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("forge said no"))

		err := client.Status(t.Context(), testUser(), testRepo(), &model.Pipeline{}, &model.Workflow{})
		require.EqualError(t, err, "forge said no")
		require.NotErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("call without deadline gives plugin unbounded context", func(t *testing.T) {
		impl := forge_mocks.NewMockForge(t)
		remoteContext := make(chan context.Context, 1)
		impl.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, _ *model.Workflow) error {
				remoteContext <- ctx
				return nil
			})

		client := newStatusRPCClient(t, impl)
		require.NoError(t, client.Status(t.Context(), testUser(), testRepo(), &model.Pipeline{}, &model.Workflow{}))

		select {
		case ctx := <-remoteContext:
			_, ok := ctx.Deadline()
			require.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("plugin Status was not called")
		}
	})
}

// testUser returns a user for the Status RPC tests.
func testUser() *model.User {
	return &model.User{ID: 1, ForgeID: 2, ForgeRemoteID: "3", Login: "octocat", Email: "octocat@example.com"}
}

// testRepo returns a repo for the Status RPC tests.
func testRepo() *model.Repo {
	return &model.Repo{ID: 4, UserID: 1, ForgeRemoteID: "5", Owner: "octocat", Name: "hello", FullName: "octocat/hello"}
}
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+// Copyright 2026 Woodpecker Authors
+//
+// Licensed under the Apache License, Version 2.0 (the "License");
+// you may not use this file except in compliance with the License.
+// You may obtain a copy of the License at
+//
+//      http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing, software
+// distributed under the License is distributed on an "AS IS" BASIS,
+// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+// See the License for the specific language governing permissions and
+// limitations under the License.
+
+package addon
+
+import (
+	"context"
+	"os"
+	"path/filepath"
+	"strconv"
+	"syscall"
+	"testing"
+	"time"
+
+	"github.com/stretchr/testify/require"
+
+	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
+	"go.woodpecker-ci.org/woodpecker/v3/server/model"
+)
+
+type closeTestForge struct{ forge.Forge }
+
+func (closeTestForge) Name() string { return strconv.Itoa(os.Getpid()) }
+
+func (closeTestForge) Close() error { return nil }
+
+func (closeTestForge) Teams(_ context.Context, u *model.User, _ *model.ListOptions) ([]*model.Team, error) {
+	if err := os.WriteFile(u.Login+".ready", nil, 0o600); err != nil {
+		return nil, err
+	}
+	for {
+		if _, err := os.Stat(u.Login + ".release"); err == nil {
+			return nil, nil
+		}
+		time.Sleep(10 * time.Millisecond)
+	}
+}
+
+func TestMain(m *testing.M) {
+	if os.Getenv(HandshakeConfig.MagicCookieKey) == HandshakeConfig.MagicCookieValue {
+		Serve(closeTestForge{})
+		return
+	}
+	os.Exit(m.Run())
+}
+
+func addonProcessAlive(pid int) bool {
+	return syscall.Kill(pid, 0) == nil
+}
+
+func TestCloseStopsAddonProcess(t *testing.T) {
+	executable, err := os.Executable()
+	require.NoError(t, err)
+
+	f, err := Load(executable)
+	require.NoError(t, err)
+	pid, err := strconv.Atoi(f.Name())
+	require.NoError(t, err)
+
+	require.NoError(t, f.Close())
+	require.Eventually(t, func() bool { return !addonProcessAlive(pid) }, 5*time.Second, 20*time.Millisecond)
+}
+
+func TestCloseWaitsForInflightAddonRPC(t *testing.T) {
+	executable, err := os.Executable()
+	require.NoError(t, err)
+	f, err := Load(executable)
+	require.NoError(t, err)
+
+	path := filepath.Join(t.TempDir(), "addon")
+	result := make(chan error, 1)
+	go func() {
+		_, err := f.Teams(t.Context(), &model.User{Login: path}, nil)
+		result <- err
+	}()
+	require.Eventually(t, func() bool {
+		_, err := os.Stat(path + ".ready")
+		return err == nil
+	}, 5*time.Second, 20*time.Millisecond)
+
+	closed := make(chan error, 1)
+	go func() { closed <- f.Close() }()
+	select {
+	case err := <-closed:
+		t.Fatalf("Close returned before the in-flight RPC finished: %v", err)
+	case <-time.After(100 * time.Millisecond):
+	}
+
+	require.NoError(t, os.WriteFile(path+".release", nil, 0o600))
+	require.NoError(t, <-result)
+	require.NoError(t, <-closed)
+}
+
+func TestCallAfterCloseFails(t *testing.T) {
+	executable, err := os.Executable()
+	require.NoError(t, err)
+	f, err := Load(executable)
+	require.NoError(t, err)
+
+	require.NoError(t, f.Close())
+	require.NoError(t, f.Close())
+	_, err = f.Teams(t.Context(), &model.User{Login: filepath.Join(t.TempDir(), "x")}, nil)
+	require.ErrorIs(t, err, ErrAddonClosed)
+}
>>>>>>> conflict 1 of 1 ends
