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
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func (s storage) TaskList() ([]*model.Task, error) {
	tasks := make([]*model.Task, 0, perPage)
	return tasks, s.engine.Find(&tasks)
}

func (s storage) TaskInsert(task *model.Task) error {
	// only Insert set auto created ID back to object
	return wrapInsert(s.engine.Insert(task))
}

// TaskInsertBatch inserts tasks in multi-row statements. The string primary
// key is set by the caller, so no ID back-fill is needed.
func (s storage) TaskInsertBatch(tasks []*model.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	size, err := s.batchSize(new(model.Task))
	if err != nil {
		return err
	}
	sess := s.engine.NewSession()
	defer sess.Close()
	if err := sess.Begin(); err != nil {
		return err
	}
	if err := insertBatched(sess, tasks, size); err != nil {
		return err
	}
	return sess.Commit()
}

func (s storage) TaskDelete(id string) error {
	return wrapDelete(s.engine.Where("id = ?", id).Delete(new(model.Task)))
}
