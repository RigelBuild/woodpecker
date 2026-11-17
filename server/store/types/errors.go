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

package types

import (
	"database/sql"
	"errors"
)

var (
	// RecordNotExist a Get or Update could not find the requested record.
	ErrRecordNotExist = sql.ErrNoRows

	// ErrInsertDuplicateDetected is returned when an insert fails because of unique constrains.
	ErrInsertDuplicateDetected = errors.New("on insert duplicate based on constraints was detected")
<<<<<<< conflict 1 of 1
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
+
+	// ErrUpdateConflict a conditional update found the row changed since it was read.
+	ErrUpdateConflict = errors.New("record changed since it was read")
+++++++ yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"

	// ErrDatabaseLocked is returned when an operation failed because the database is locked (usually a transitory state).
	ErrDatabaseLocked = errors.New("database is locked")
>>>>>>> conflict 1 of 1 ends
)
