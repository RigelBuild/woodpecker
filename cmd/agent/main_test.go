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

package main

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForwardFirstStopsBeforeForwarding(t *testing.T) {
	notified := make(chan os.Signal, 1)
	stopped := make(chan struct{})
	first := forwardFirst(notified, func() { close(stopped) })

	notified <- syscall.SIGTERM
	assert.Equal(t, syscall.SIGTERM, <-first)
	select {
	case <-stopped:
	default:
		t.Fatal("signal forwarded before default handling was restored")
	}
}
