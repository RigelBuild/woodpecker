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

var addStepSkipExitCode = xormigrate.Migration{
	ID: "add-step-skip-exit-code",
	MigrateSession: func(sess *xorm.Session) error {
		if _, err := sess.Exec("ALTER TABLE steps ADD COLUMN step_skip_exit_code INTEGER DEFAULT 0"); err != nil {
			return fmt.Errorf("add step skip exit code column: %w", err)
		}
		return nil
	},
}
