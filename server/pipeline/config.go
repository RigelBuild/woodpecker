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
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// findOrPersistPipelineConfigs stores the pipeline's config files and returns
// the stored rows in the order of forgeYamlConfigs.
func findOrPersistPipelineConfigs(store store.Store, currentPipeline *model.Pipeline, forgeYamlConfigs []*forge_types.FileMeta) ([]*model.Config, error) {
	if len(forgeYamlConfigs) == 0 {
		return []*model.Config{}, nil
	}
	configs := make([]*model.Config, 0, len(forgeYamlConfigs))
	for _, forgeYamlConfig := range forgeYamlConfigs {
		configs = append(configs, &model.Config{
			RepoID: currentPipeline.RepoID,
			Name:   builder.SanitizePath(forgeYamlConfig.Name),
			Data:   forgeYamlConfig.Data,
		})
	}
	return store.ConfigsPersist(currentPipeline.RepoID, configs)
}

func linkPipelineConfigs(store store.Store, configs []*model.Config, pipelineID int64) error {
	if len(configs) == 0 {
		return nil
	}
	links := make([]*model.PipelineConfig, 0, len(configs))
	for _, conf := range configs {
		links = append(links, &model.PipelineConfig{
			ConfigID:   conf.ID,
			PipelineID: pipelineID,
		})
	}
	return store.PipelineConfigsCreate(links)
}
