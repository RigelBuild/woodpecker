// Copyright 2021 Woodpecker Authors
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

package datastore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"slices"

	"xorm.io/builder"
	"xorm.io/xorm"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func (s storage) ConfigsForPipeline(pipelineID int64) ([]*model.Config, error) {
	configs := make([]*model.Config, 0, perPage)
	return configs, s.engine.
		Table("configs").
		Join("LEFT", "pipeline_configs", "configs.id = pipeline_configs.config_id").
		Where("pipeline_configs.pipeline_id = ?", pipelineID).
		Find(&configs)
}

func (s storage) configFindIdentical(sess *xorm.Session, repoID int64, hash, name string) (*model.Config, error) {
	conf := new(model.Config)
	if err := wrapGet(sess.Where(
		builder.Eq{"repo_id": repoID, "hash": hash, "name": name},
	).Get(conf)); err != nil {
		return nil, err
	}
	return conf, nil
}

func (s storage) ConfigPersist(conf *model.Config) (*model.Config, error) {
	conf.Hash = fmt.Sprintf("%x", sha256.Sum256(conf.Data))

	// no transaction: it would not stop a concurrent insert anyway, and on postgres a
	// failed insert aborts the transaction, which would break the lookup below
	sess := s.engine.NewSession()
	defer sess.Close()

	existingConfig, err := s.configFindIdentical(sess, conf.RepoID, conf.Hash, conf.Name)
	if err != nil && !errors.Is(err, types.ErrRecordNotExist) {
		return nil, err
	}
	if existingConfig != nil {
		return existingConfig, nil
	}

	err = s.configCreate(sess, conf)
	if errors.Is(err, types.ErrInsertDuplicateDetected) {
		// a concurrent request persisted the identical config after our lookup,
		// so use that one (see https://github.com/woodpecker-ci/woodpecker/issues/7173)
		return s.configFindIdentical(sess, conf.RepoID, conf.Hash, conf.Name)
	}
	if err != nil {
		return nil, err
	}

	return conf, nil
}

// ConfigsPersist is ConfigPersist for many configs of one repo in a bounded
// number of queries. It returns the stored configs in input order.
func (s storage) ConfigsPersist(repoID int64, confs []*model.Config) ([]*model.Config, error) {
	if len(confs) == 0 {
		return []*model.Config{}, nil
	}
	for _, conf := range confs {
		if conf.Name == "" {
			return nil, fmt.Errorf("insert config to store failed: 'Name' has to be set")
		}
		conf.RepoID = repoID
		conf.Hash = fmt.Sprintf("%x", sha256.Sum256(conf.Data))
	}

	// no transaction, for the same reason as in ConfigPersist
	sess := s.engine.NewSession()
	defer sess.Close()

	stored, err := s.configsFindIdentical(sess, repoID, confs)
	if err != nil {
		return nil, err
	}

	var missing []*model.Config
	queued := make(map[string]struct{})
	for _, conf := range confs {
		key := configKey(conf.Hash, conf.Name)
		if _, ok := stored[key]; ok {
			continue
		}
		if _, ok := queued[key]; ok {
			continue
		}
		queued[key] = struct{}{}
		missing = append(missing, conf)
	}

	if len(missing) > 0 {
		size, err := s.batchSize(new(model.Config))
		if err != nil {
			return nil, err
		}
		for chunk := range slices.Chunk(missing, size) {
			err := wrapInsert(sess.Insert(chunk))
			if errors.Is(err, types.ErrInsertDuplicateDetected) {
				// a concurrent request persisted one of these configs after our lookup;
				// ConfigPersist resolves that per row
				for _, conf := range chunk {
					if _, err := s.ConfigPersist(conf); err != nil {
						return nil, err
					}
				}
				continue
			}
			if err != nil {
				return nil, err
			}
		}

		// slice inserts do not back-fill IDs, so read the new rows back
		inserted, err := s.configsFindIdentical(sess, repoID, missing)
		if err != nil {
			return nil, err
		}
		maps.Copy(stored, inserted)
	}

	result := make([]*model.Config, len(confs))
	for i, conf := range confs {
		found, ok := stored[configKey(conf.Hash, conf.Name)]
		if !ok {
			return nil, fmt.Errorf("config %q of repo %d not found after insert", conf.Name, repoID)
		}
		result[i] = found
	}
	return result, nil
}

// configsFindIdentical loads the stored configs that match confs on hash and
// name, keyed by configKey, with one query per chunk of distinct hashes.
func (s storage) configsFindIdentical(sess *xorm.Session, repoID int64, confs []*model.Config) (map[string]*model.Config, error) {
	wanted := make(map[string]struct{}, len(confs))
	seenHashes := make(map[string]struct{}, len(confs))
	hashes := make([]string, 0, len(confs))
	for _, conf := range confs {
		wanted[configKey(conf.Hash, conf.Name)] = struct{}{}
		if _, ok := seenHashes[conf.Hash]; !ok {
			seenHashes[conf.Hash] = struct{}{}
			hashes = append(hashes, conf.Hash)
		}
	}

	found := make(map[string]*model.Config, len(confs))
	// one bind parameter per hash plus the repo ID
	for chunk := range slices.Chunk(hashes, pgBatchSize) {
		rows := make([]*model.Config, 0, len(chunk))
		if err := sess.Where(builder.Eq{"repo_id": repoID}.And(builder.In("hash", chunk))).Find(&rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			key := configKey(row.Hash, row.Name)
			if _, ok := wanted[key]; ok {
				found[key] = row
			}
		}
	}
	return found, nil
}

func configKey(hash, name string) string {
	return hash + "\x00" + name
}

func (s storage) configCreate(sess *xorm.Session, config *model.Config) error {
	// should never happen but just in case
	if config.Name == "" {
		return fmt.Errorf("insert config to store failed: 'Name' has to be set")
	}
	if config.Hash == "" {
		return fmt.Errorf("insert config to store failed: 'Hash' has to be set")
	}

	// only Insert set auto created ID back to object
	return wrapInsert(sess.Insert(config))
}

func (s storage) PipelineConfigCreate(config *model.PipelineConfig) error {
	// only Insert set auto created ID back to object
	return wrapInsert(s.engine.Insert(config))
}

// PipelineConfigsCreate links configs to pipelines in multi-row statements.
func (s storage) PipelineConfigsCreate(links []*model.PipelineConfig) error {
	if len(links) == 0 {
		return nil
	}
	size, err := s.batchSize(new(model.PipelineConfig))
	if err != nil {
		return err
	}
	sess := s.engine.NewSession()
	defer sess.Close()
	return insertBatched(sess, links, size)
}
