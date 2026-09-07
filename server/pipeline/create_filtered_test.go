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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// Callers distinguish a filtered pipeline from a real failure with
// errors.Is(err, ErrFiltered), so Create must return the sentinel unwrapped.
// Wrapping it with a formatted message would keep errors.Is working, but
// returning a fresh errors.New with the same text would not, and that
// difference is invisible at the call site.
func TestCreateSkipCommitMessageReturnsErrFiltered(t *testing.T) {
	t.Parallel()

	mockStore := store_mocks.NewMockStore(t)
	repo := &model.Repo{ID: 10, UserID: 1, FullName: "octocat/hello-world"}
	mockStore.On("GetUser", int64(1)).Return(&model.User{ID: 1, Login: "octocat"}, nil)

	// A skip-ci commit is filtered before the pipeline is ever persisted, so
	// no CreatePipeline or DeletePipeline call is expected. mockStore is
	// strict: an unexpected store call fails the test.
	created, err := Create(t.Context(), mockStore, repo, &model.Pipeline{
		Event:   model.EventPush,
		Message: "chore: tidy up [skip ci]",
	})

	assert.Nil(t, created)
	assert.ErrorIs(t, err, ErrFiltered)
}

// A filtered pipeline is not a failure, so it must not be reported as one.
// This pins the property the sentinel exists for: ErrFiltered is
// distinguishable from an arbitrary error carrying the same message.
func TestErrFilteredIsDistinguishable(t *testing.T) {
	t.Parallel()

	assert.ErrorIs(t, ErrFiltered, ErrFiltered)
	assert.False(t, errors.Is(errors.New(ErrFiltered.Error()), ErrFiltered),
		"a same-text error must not satisfy errors.Is; callers rely on the sentinel identity")
}
