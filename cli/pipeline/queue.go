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
	"context"
	"fmt"
	"io"
	"os"
	"text/template"

	"github.com/urfave/cli/v3"

	"go.woodpecker-ci.org/woodpecker/v3/cli/common"
	"go.woodpecker-ci.org/woodpecker/v3/cli/internal"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

var pipelineQueueCmd = &cli.Command{
	Name:      "queue",
	Usage:     "show pipeline queue",
	ArgsUsage: " ",
	Action:    pipelineQueue,
	Flags:     []cli.Flag{common.FormatFlag(tmplPipelineQueue, false)},
}

func pipelineQueue(ctx context.Context, c *cli.Command) error {
	client, err := internal.NewClient(ctx, c)
	if err != nil {
		return err
	}

	return pipelineQueueOutput(c, client, os.Stdout)
}

func pipelineQueueOutput(c *cli.Command, client woodpecker.Client, out io.Writer) error {
	pipelines, err := client.PipelineQueue()
	if err != nil {
		return err
	}

	if len(pipelines) == 0 {
		_, err := fmt.Fprintln(out, "there are no pending or running pipelines")
		return err
	}

	tmpl, err := template.New("_").Parse(c.String("format") + "\n")
	if err != nil {
		return err
	}

	for _, pipeline := range pipelines {
		if err := tmpl.Execute(out, pipeline); err != nil {
			return err
		}
	}
	return nil
}

// Template for pipeline list information.
var tmplPipelineQueue = "\x1b[33mrepo:{{ .RepoID }} #{{ .Number }} \x1b[0m" + `
Status: {{ .Status }}
Event: {{ .Event }}
Commit: {{ .Commit }}
Branch: {{ .Branch }}
Ref: {{ .Ref }}
Author: {{ .Author }} {{ if .Email }}<{{.Email}}>{{ end }}
Message: {{ .Message }}
`
