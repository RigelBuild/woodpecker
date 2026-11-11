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

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.woodpecker-ci.org/woodpecker/v3/cmd/agent/core"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/docker"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/kubernetes"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/local"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/shared/dot_env"
)

var backends = []backend_types.Backend{
	kubernetes.New(),
	docker.New(),
	local.New(),
}

func main() {
	dot_env.Load()

	ctx := core.AgentRootContext(context.Background(), firstSignal())
	core.RunAgent(ctx, backends)
}

// firstSignal forwards one SIGINT/SIGTERM, then restores default handling so a
// second signal kills a stuck shutdown.
func firstSignal() <-chan os.Signal {
	notified := make(chan os.Signal, 1)
	signal.Notify(notified, syscall.SIGINT, syscall.SIGTERM)
	return forwardFirst(notified, func() { signal.Stop(notified) })
}

func forwardFirst(notified <-chan os.Signal, stop func()) <-chan os.Signal {
	first := make(chan os.Signal, 1)
	go func() {
		sig := <-notified
		stop()
		first <- sig
	}()
	return first
}
