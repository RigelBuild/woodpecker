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

package migration

import (
	"fmt"

	"src.techknowlogick.com/xormigrate"
	"xorm.io/xorm"
)

var addSecretVersion = xormigrate.Migration{
	ID: "add-secret-version",
	MigrateSession: func(sess *xorm.Session) (err error) {
		type secrets struct {
			ID      int64 `xorm:"pk autoincr 'id'"`
			Version int64 `xorm:"version 'version'"`
		}

		if err := sess.Sync(new(secrets)); err != nil {
			return fmt.Errorf("sync models failed: %w", err)
		}
		return nil
	},
}
