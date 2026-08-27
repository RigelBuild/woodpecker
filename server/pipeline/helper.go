// Copyright 2022 Woodpecker Authors
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
	"context"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

func updatePipelineStatus(ctx context.Context, _forge forge.Forge, _store store.Store, pipeline *model.Pipeline, repo *model.Repo, user *model.User) {
	// Per-workflow status is opt-out (StatusPerWorkflow, default on). On an
	// affected-aware fan-out a pipeline can carry dozens of workflows, each an
	// extra forge write that pressures the forge's rate limit; disabling it
	// leaves only the pipeline-level aggregate below (the required check).
	if server.Config.Server.StatusPerWorkflow {
		for _, workflow := range pipeline.Workflows {
			if err := _forge.Status(ctx, user, repo, pipeline, workflow); err != nil {
				// A per-workflow status failure must not abort the loop: the
				// pipeline-level aggregate below is the required branch-protection
				// check, so it has to run even if one workflow's report failed
				// (otherwise a single throttled POST leaves the check stuck pending).
				log.Error().Err(err).Msgf("error setting commit status for %s/%d", repo.FullName, pipeline.Number)
			}
		}
	}

	if server.Config.Server.StatusAggregate {
		if err := forge.ReportAggregateStatus(ctx, _forge, _store, user, repo, pipeline); err != nil {
			log.Error().Err(err).Msgf("error setting aggregate status for %s/%d", repo.FullName, pipeline.Number)
		}
	}

	if server.Config.Server.StatusMeta {
		if err := forge.ReportMetaStatus(ctx, _forge, _store, user, repo, pipeline); err != nil {
			log.Error().Err(err).Msgf("error setting meta status for %s/%d", repo.FullName, pipeline.Number)
		}
	}
}
