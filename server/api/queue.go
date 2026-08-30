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

package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// slowHandlerWriteExtension is the per-response write-deadline extension taken
// by the handlers in this package that can legitimately run longer than the
// server-wide WriteTimeout — the queue long-poll below, the all-repo repair,
// and the login-time permission sync, each of which is bounded by external
// work (a scheduler draining, a forge answering) rather than by its own
// compute. See writedeadline.go for why that budget covers handler runtime and
// not just blocked writes.
//
// It is sized at roughly the server-wide WriteTimeout on purpose: a handler
// that keeps making progress re-arms before the previous extension lapses and
// so never trips, while one that wedges between two re-arm points still dies
// on a budget no larger than the server-wide one. Making it much shorter would
// trip handlers whose next unit of work is a slow-but-healthy forge call;
// making it much longer would let a wedged handler hold a connection well past
// what the server-wide timeout promises.
const slowHandlerWriteExtension = 60 * time.Second

// queuePollInterval paces the queue long-poll below. The loop used to spin hot
// on the scheduler, burning a core for the whole wait; it now sleeps between
// probes, which also gives it a sane cadence at which to re-arm its write
// deadline instead of issuing that syscall millions of times a second. Short
// enough that the observed latency of the 204 is dominated by the queue
// draining, not by the poll.
const queuePollInterval = 100 * time.Millisecond

// GetQueueInfo
//
//	@Summary		Get pipeline queue information
//	@Description	Returns pipeline queue information with agent details
//	@Router			/queue/info [get]
//	@Produce		json
//	@Success		200	{object}	QueueInfo
//	@Tags			Pipeline queues
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
func GetQueueInfo(c *gin.Context) {
	info := server.Config.Services.Scheduler.Info(c)
	_store := store.FromContext(c)

	// Create a map to store agent names by ID
	agentNameMap := make(map[int64]string)

	// Process tasks and add agent names
	pendingWithAgents, err := processQueueTasks(_store, info.Pending, agentNameMap)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	waitingWithAgents, err := processQueueTasks(_store, info.WaitingOnDeps, agentNameMap)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	runningWithAgents, err := processQueueTasks(_store, info.Running, agentNameMap)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	// Create response with agent-enhanced tasks
	response := model.QueueInfo{
		Pending:       pendingWithAgents,
		WaitingOnDeps: waitingWithAgents,
		Running:       runningWithAgents,
		Stats: struct {
			WorkerCount        int `json:"worker_count"`
			PendingCount       int `json:"pending_count"`
			WaitingOnDepsCount int `json:"waiting_on_deps_count"`
			RunningCount       int `json:"running_count"`
		}{
			WorkerCount:        info.Stats.Workers,
			PendingCount:       info.Stats.Pending,
			WaitingOnDepsCount: info.Stats.WaitingOnDeps,
			RunningCount:       info.Stats.Running,
		},
		Paused: info.Paused,
	}

	c.IndentedJSON(http.StatusOK, response)
}

// PauseQueue
//
//	@Summary	Pause the pipeline queue
//	@Router		/queue/pause [post]
//	@Produce	plain
//	@Success	204
//	@Tags		Pipeline queues
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
func PauseQueue(c *gin.Context) {
	server.Config.Services.Scheduler.Pause()
	c.Status(http.StatusNoContent)
}

// ResumeQueue
//
//	@Summary	Resume the pipeline queue
//	@Router		/queue/resume [post]
//	@Produce	plain
//	@Success	204
//	@Tags		Pipeline queues
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
func ResumeQueue(c *gin.Context) {
	server.Config.Services.Scheduler.Resume()
	c.Status(http.StatusNoContent)
}

// BlockTilQueueHasRunningItem
//
//	@Summary	Block til pipeline queue has a running item
//	@Router		/queue/norunningpipelines [get]
//	@Produce	plain
//	@Success	204
//	@Tags		Pipeline queues
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
func BlockTilQueueHasRunningItem(c *gin.Context) {
	// This long-poll returns only once the queue has drained, which can take far
	// longer than the server-wide WriteTimeout. That budget covers total handler
	// runtime, so without a rolling per-response deadline the connection is torn
	// down mid-wait and the documented 204 never reaches the client.
	//
	// Rolling the deadline removes the only bound this handler had, so it must
	// supply its own: the loop writes nothing, so no write can ever trip the
	// deadline and end it. The request context is the replacement — a client
	// hang-up ends the wait immediately. Without it, every abandoned request
	// would pin a goroutine and a 10 Hz poll on the scheduler lock until the
	// process restarted.
	progress := slowHandlerProgress(c, newResponseController(c.Writer))
	for {
		// A canceled request ends the wait: there is nobody to send the 204 to.
		if err := progress(); err != nil {
			return
		}

		info := server.Config.Services.Scheduler.Info(c)
		if info.Stats.Running == 0 {
			break
		}

		select {
		case <-c.Request.Context().Done():
			return
		case <-time.After(queuePollInterval):
		}
	}
	c.Status(http.StatusNoContent)
}

// processQueueTasks converts tasks to QueueTask structs and adds agent names.
func processQueueTasks(store store.Store, tasks []*model.Task, agentNameMap map[int64]string) ([]model.QueueTask, error) {
	result := make([]model.QueueTask, 0, len(tasks))

	for _, task := range tasks {
		taskResponse := model.QueueTask{
			Task: *task,
		}

		if task.AgentID != 0 {
			name, ok := getAgentName(store, agentNameMap, task.AgentID)
			if !ok {
				log.Error().Msgf("agent not found for task %s", task.ID)
			}

			taskResponse.AgentName = name
		}

		if task.PipelineID != 0 {
			p, err := store.GetPipeline(task.PipelineID)
			if err != nil {
				return nil, fmt.Errorf("pipeline not found for task %s", task.ID)
			}

			taskResponse.PipelineNumber = p.Number
		}

		result = append(result, taskResponse)
	}
	return result, nil
}
