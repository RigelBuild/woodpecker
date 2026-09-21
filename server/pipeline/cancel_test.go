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
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestCancelPreviousPipelinesOnlyCancelsOlderPipeline(t *testing.T) {
	tests := []struct {
		name             string
		currentNumber    int64
		activeNumber     int64
		wantCancel       bool
		wantSupersededBy int64
	}{
		{
			name:          "older start cannot cancel newer active pipeline",
			currentNumber: 27344,
			activeNumber:  27347,
		},
		{
			name:             "newer start cancels older active pipeline",
			currentNumber:    27347,
			activeNumber:     27344,
			wantCancel:       true,
			wantSupersededBy: 27347,
		},
		{
			name:          "equal number cannot cancel different pipeline",
			currentNumber: 27344,
			activeNumber:  27344,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &model.Repo{
				Owner:                        "o",
				Name:                         "r",
				FullName:                     "o/r",
				CancelPreviousPipelineEvents: []model.WebhookEvent{model.EventPull},
			}
			current := &model.Pipeline{ID: 1, Number: tt.currentNumber, Event: model.EventPull, Refspec: "refs/pull/123/head"}
			active := &model.Pipeline{ID: 2, Number: tt.activeNumber, Event: model.EventPull, Refspec: "refs/pull/123/head", Status: model.StatusRunning}

			storeMock := store_mocks.NewMockStore(t)
			storeMock.EXPECT().GetActivePipelineList(repo).Return([]*model.Pipeline{active}, nil)
			schedulerMock := mocks.NewMockScheduler(t)
			if tt.wantCancel {
				storeMock.EXPECT().WorkflowGetTree(mock.MatchedBy(func(p *model.Pipeline) bool { return p.ID == active.ID })).Return([]*model.Workflow{}, nil)
				storeMock.EXPECT().UpdatePipeline(mock.MatchedBy(func(p *model.Pipeline) bool {
					return p.ID == active.ID && p.CancelInfo != nil && p.CancelInfo.SupersededBy == tt.wantSupersededBy
				})).Return(nil)
				schedulerMock.EXPECT().CancelWorkflows(mock.Anything, mock.Anything).Return(nil)
				schedulerMock.EXPECT().PublishPipelineEvent(mock.Anything, repo, mock.Anything).Return(nil)
			}
			origScheduler := server.Config.Services.Scheduler
			server.Config.Services.Scheduler = schedulerMock
			t.Cleanup(func() { server.Config.Services.Scheduler = origScheduler })

			err := cancelPreviousPipelines(context.Background(), nil, storeMock, current, repo, &model.User{})
			require.NoError(t, err)
		})
	}
}
