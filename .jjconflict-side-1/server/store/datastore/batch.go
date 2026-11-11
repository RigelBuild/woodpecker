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

package datastore

import (
	"slices"

	"xorm.io/xorm"
)

// maxBindParams stays under sqlite's 32766 host-parameter limit, the lowest of
// the supported drivers.
const maxBindParams = 30000

// batchSize is the most rows of bean one multi-row statement may carry: at most
// pgBatchSize rows and maxBindParams bind parameters.
func (s storage) batchSize(bean any) (int, error) {
	table, err := s.engine.TableInfo(bean)
	if err != nil {
		return 0, err
	}
	return min(pgBatchSize, maxBindParams/len(table.Columns())), nil
}

// insertBatched inserts rows with one multi-row INSERT per chunk of size rows.
// It leaves auto-increment IDs unset: xorm does not back-fill them on slice inserts.
func insertBatched[T any](sess *xorm.Session, rows []T, size int) error {
	for chunk := range slices.Chunk(rows, size) {
		if err := wrapInsert(sess.Insert(chunk)); err != nil {
			return err
		}
	}
	return nil
}

// idByPID is the projection the workflow and step ID back-fill reads.
type idByPID struct {
	ID  int64 `xorm:"'id'"`
	PID int   `xorm:"'pid'"`
}

// idsByPID maps pid to id for every row of table in one pipeline. Both
// workflows and steps are unique on (pipeline_id, pid).
func idsByPID(sess *xorm.Session, table string, pipelineID int64) (map[int]int64, error) {
	rows := make([]idByPID, 0)
	if err := sess.Table(table).Cols("id", "pid").Where("pipeline_id = ?", pipelineID).Find(&rows); err != nil {
		return nil, err
	}
	ids := make(map[int]int64, len(rows))
	for _, row := range rows {
		ids[row.PID] = row.ID
	}
	return ids, nil
}
