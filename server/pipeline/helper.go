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

	// Stale-pending guard (RIG-1170). GitHub commit-status is last-write-wins per
	// context, so a non-terminal ("pending"-mapped) post that lands AFTER a
	// terminal one silently wedges the required check pending forever. That is
	// exactly what two concurrent same-commit handlers produce: each cancels the
	// other (posting a terminal status) and then posts its OWN creation-time
	// pending, so the last pending wins even though every pipeline is terminal.
	//
	// So: when the pipeline we are about to report is non-terminal, re-read it
	// from the store and skip the shared-context posts if the STORED state has
	// since gone terminal — our in-memory copy is stale. Guarding here covers
	// every caller of this shared poster at one site: start.go (the observed
	// stale writer), cancel.go, decline.go and restart.go. The agent-driven rpc
	// path posts through its own updateForgeStatus and does not route here.
	//
	// Deliberately narrow:
	//   - Skip-only. It never invents or upgrades a status, and never gates a
	//     terminal post; a genuinely-pending pipeline still posts.
	//   - Only the shared-context reports (aggregate + meta) are suppressed. The
	//     per-workflow loop above writes workflow-scoped contexts, which are not
	//     the wedged required check.
	//   - Fail-open. A store error or an unpersisted pipeline (ID 0) posts as
	//     before: losing a report is worse than a redundant one.
	//   - Honest residue: a re-read narrows but cannot close the TOCTOU window (a
	//     cancel can commit between the read and the POST flush). The ingest dedup
	//     window (server/api/hook.go) is what removes the competing writer; this
	//     is defense in depth.
	if !pipeline.Status.IsTerminal() && pipeline.ID != 0 {
		stored, err := _store.GetPipeline(pipeline.ID)
		switch {
		case err != nil:
			log.Error().Err(err).Msgf("stale-pending guard: cannot re-read pipeline %s/%d, posting anyway", repo.FullName, pipeline.Number)
		case stored.Status.IsTerminal():
			log.Debug().
				Str("repo", repo.FullName).
				Int64("pipeline", pipeline.Number).
				Str("in-memory", string(pipeline.Status)).
				Str("stored", string(stored.Status)).
				Msg("skipping stale non-terminal status post: the stored pipeline is already terminal")
			return
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
