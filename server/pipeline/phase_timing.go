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

package pipeline

import (
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// startPhase starts timing one phase of pipeline creation. The returned
// function logs one debug line with the phase name, its duration, and the
// repo and pipeline identity. Pass the phase's error, or nil on success.
func startPhase(repo *model.Repo, pipeline *model.Pipeline, phase string) func(err error) {
	start := time.Now()
	return func(err error) {
		ev := log.Debug().
			Str("phase", phase).
			Dur("duration", time.Since(start)).
			Int64("repo_id", repo.ID).
			Int64("number", pipeline.Number)
		if err != nil {
			ev = ev.Err(err)
		}
		ev.Msg("pipeline create phase")
	}
}
