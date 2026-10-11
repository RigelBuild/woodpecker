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

package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// perWorkflowGateForge is a hand-written fake for exercising updateForgeStatus's
// StatusPerWorkflow gate. Like helper_test's aggregateResilienceForge, it must be
// hand-written rather than the mockery MockForge: the pipeline-level aggregate is
// reported via forge.ReportAggregateStatus, which type-asserts to the optional
// forge.AggregateStatusReporter (the StatusAggregate method). A MockForge does not
// satisfy that assertion, so ReportAggregateStatus would silently return nil and
// the aggregate call could never be observed. This fake implements both Status and
// StatusAggregate so each call is counted.
//
// It embeds forge.Forge only to satisfy the interface for the methods
// updateForgeStatus never touches; any unexpected call lands on the nil embedded
// interface and panics, surfacing accidental new forge dependencies loudly. It
// deliberately does NOT implement forge.Refresher, so forge.Refresh is a no-op.
type perWorkflowGateForge struct {
	forge.Forge

	statusCalledFor []int64
	aggregateCalls  int
}

func (f *perWorkflowGateForge) Status(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, workflow *model.Workflow) error {
	f.statusCalledFor = append(f.statusCalledFor, workflow.ID)
	return nil
}

func (f *perWorkflowGateForge) StatusAggregate(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, _ []*model.Workflow) error {
	f.aggregateCalls++
	return nil
}

// setStatusFlags toggles the process-global StatusPerWorkflow flag for the
// duration of a test, restoring the previous value on cleanup. StatusAggregate
// is forced on: every caller exercises a path that needs the aggregate report,
// so it is not a parameter.
func setStatusFlags(t *testing.T, perWorkflow bool) {
	t.Helper()
	origPerWorkflow := server.Config.Server.StatusPerWorkflow
	origAggregate := server.Config.Server.StatusAggregate
	server.Config.Server.StatusPerWorkflow = perWorkflow
	server.Config.Server.StatusAggregate = true
	t.Cleanup(func() {
		server.Config.Server.StatusPerWorkflow = origPerWorkflow
		server.Config.Server.StatusAggregate = origAggregate
	})
}

// newForgeStatusFixture wires an RPC whose forge is the supplied fake: the store
// returns a real user (so updateForgeStatus proceeds past the GetUser lookup to
// the gate) and the services Manager hands back f from ForgeFromRepo. The Manager
// is restored on cleanup so the process-global isn't leaked to other tests.
func newForgeStatusFixture(t *testing.T, f forge.Forge) RPC {
	t.Helper()

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("GetUser", mock.Anything).Return(&model.User{ID: 1, AccessToken: "x"}, nil)

	mockManager := manager_mocks.NewMockManager(t)
	mockManager.On("ForgeFromRepo", mock.Anything).Return(f, nil)
	origManager := server.Config.Services.Manager
	server.Config.Services.Manager = mockManager
	t.Cleanup(func() { server.Config.Services.Manager = origManager })

	return newTestRPC(t, mockStore, nil)
}

// TestUpdateForgeStatusPerWorkflowGate pins the StatusPerWorkflow gate in the rpc
// path (rpc.go updateForgeStatus): the per-workflow _forge.Status call is wrapped
// in `if workflow != nil && server.Config.Server.StatusPerWorkflow`, while the
// pipeline-level aggregate below it fires regardless. The three cases together
// fully pin that conjunction — each redden if a half of the guard is dropped.
func TestUpdateForgeStatusPerWorkflowGate(t *testing.T) {
	// Gate off: a completed workflow must NOT be reported per-workflow, but the
	// aggregate (the required branch-protection check) must still fire. This is
	// the whole point of the flag — collapse the fan-out to a single forge write.
	// Drop the StatusPerWorkflow half of the guard and statusCalledFor gains 30.
	t.Run("per-workflow status gated off, aggregate still fires", func(t *testing.T) {
		setStatusFlags(t, false)

		f := &perWorkflowGateForge{}
		rpcInst := newForgeStatusFixture(t, f)

		rpcInst.updateForgeStatus(context.Background(), defaultRepo(), defaultPipeline(model.StatusSuccess), defaultWorkflow(model.StatusSuccess))

		require.Empty(t, f.statusCalledFor,
			"with StatusPerWorkflow off, no per-workflow Status must be reported")
		assert.Equal(t, 1, f.aggregateCalls,
			"the pipeline-level aggregate must still fire exactly once regardless of StatusPerWorkflow")
	})

	// Gate on (upstream default): the per-workflow report fires once for the
	// workflow, and the aggregate still fires. This keeps the gated-off assertion
	// honest — it proves the flag drives the per-workflow call, not that Status
	// is simply never reached.
	t.Run("per-workflow status fires when gate on, aggregate too", func(t *testing.T) {
		setStatusFlags(t, true)

		f := &perWorkflowGateForge{}
		rpcInst := newForgeStatusFixture(t, f)
		workflow := defaultWorkflow(model.StatusSuccess)

		rpcInst.updateForgeStatus(context.Background(), defaultRepo(), defaultPipeline(model.StatusSuccess), workflow)

		assert.Equal(t, []int64{workflow.ID}, f.statusCalledFor,
			"with StatusPerWorkflow on, the workflow must be reported per-workflow exactly once")
		assert.Equal(t, 1, f.aggregateCalls,
			"the aggregate must still fire exactly once when StatusPerWorkflow is on")
	})

	// nil workflow (pipeline-level report, e.g. from Init/Done): even with the
	// gate on, a per-workflow Status must not be attempted — the guard's
	// `workflow != nil` half protects the nil deref. The aggregate still fires.
	// Drop that half and _forge.Status(…, nil) dereferences a nil workflow.
	t.Run("nil workflow never reports per-workflow even with gate on", func(t *testing.T) {
		setStatusFlags(t, true)

		f := &perWorkflowGateForge{}
		rpcInst := newForgeStatusFixture(t, f)

		rpcInst.updateForgeStatus(context.Background(), defaultRepo(), defaultPipeline(model.StatusSuccess), nil)

		require.Empty(t, f.statusCalledFor,
			"a nil workflow must never trigger a per-workflow Status, even with StatusPerWorkflow on")
		assert.Equal(t, 1, f.aggregateCalls,
			"the aggregate must still fire once on a pipeline-level (nil workflow) report")
	})
}
