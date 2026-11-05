// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubernetes

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kube_core_v1 "k8s.io/api/core/v1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	kube_meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"
)

func TestTailLogsReopensAndDeduplicatesOverlappingStreams(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-overlap")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})

	times := []string{
		"2026-11-05T12:00:00.100Z",
		"2026-11-05T12:00:00.200Z",
		"2026-11-05T12:00:00.300Z",
		"2026-11-05T12:00:00.400Z",
		"2026-11-05T12:00:00.500Z",
	}
	streams := []string{
		times[0] + " first\n" + times[1] + " second\n",
		times[0] + " first\n" + times[1] + " second\n" + times[2] + " third\n",
		times[1] + " second\n" + times[2] + " third\n" + times[3] + " fourth\n",
		times[3] + " fourth\n" + times[4] + " fifth\n",
	}
	var opened []*kube_core_v1.PodLogOptions
	engine.openLogStream = func(_ context.Context, _, _ string, opts *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		opened = append(opened, opts.DeepCopy())
		if len(opened) > len(streams) {
			return io.NopCloser(strings.NewReader("")), nil
		}
		return io.NopCloser(strings.NewReader(streams[len(opened)-1])), nil
	}

	getCount := 0
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		getCount++
		state := kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}
		if getCount >= 3 {
			state = kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}
		}
		return true, tailTestPod(podName, state), nil
	})

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "first\nsecond\nthird\nfourth\nfifth\n", string(got))
	if assert.Len(t, opened, 4) {
		assert.True(t, opened[0].Timestamps)
		assert.True(t, opened[0].Follow)
		assert.Nil(t, opened[0].SinceTime)
		for i, timestamp := range times[1:4] {
			assert.True(t, opened[i+1].Timestamps)
			expected, parseErr := time.Parse(time.RFC3339Nano, timestamp)
			require.NoError(t, parseErr)
			assert.True(t, opened[i+1].SinceTime.Time.Equal(expected))
		}
	}
}

func TestTailLogsDrainsFinalTerminatedStream(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-terminated")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	streams := []string{
		"2026-11-05T12:01:00.100Z start\n",
		"2026-11-05T12:01:00.200Z recovered tail\n",
	}
	var opened []*kube_core_v1.PodLogOptions
	engine.openLogStream = func(_ context.Context, _, _ string, opts *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		opened = append(opened, opts.DeepCopy())
		return io.NopCloser(strings.NewReader(streams[len(opened)-1])), nil
	}
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 1}}), nil
	})

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "start\nrecovered tail\n", string(got))
	assert.Len(t, opened, 2)
	assert.NotNil(t, opened[1].SinceTime)
}

func TestTailLogsFinishesWhenPodIsDeleted(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-deleted")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("2026-11-05T12:02:00.100Z before delete\n")), nil
	}
	getCount := 0
	client.PrependReactor("get", "pods", func(action kube_testing.Action) (bool, kube_runtime.Object, error) {
		getCount++
		return true, nil, kube_errors.NewNotFound(action.GetResource().GroupResource(), podName)
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	reader, err := engine.tailLogs(ctx, step, podName)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(reader)
		result <- readErr
	}()
	select {
	case err := <-result:
		assert.NoError(t, err)
	case <-ctx.Done():
		assert.Equal(t, 1, getCount)
	}
}

func TestTailLogsPassesFinalLineWithoutNewline(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-no-newline")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("2026-11-05T12:03:00.100Z final line")), nil
	}
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}), nil
	})

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "final line", string(got))
}

func TestTailLogsWritesRepeatedTimestampsWithinStream(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-repeated-timestamp")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	streams := []string{
		"2026-11-05T12:04:00.100Z first\n2026-11-05T12:04:00.100Z second\n",
		"",
	}
	streamIndex := 0
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		stream := streams[streamIndex]
		streamIndex++
		return io.NopCloser(strings.NewReader(stream)), nil
	}
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}), nil
	})

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "first\nsecond\n", string(got))
}

func TestTailLogsPassesMalformedTimestampLinesThrough(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{name: "missing separator", line: "malformed\n"},
		{name: "invalid timestamp", line: "not-a-timestamp message\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewClientset()
			engine := makeEngine(client)
			step := makeStep("tail-malformed")
			podName, err := stepToPodName(step)
			require.NoError(t, err)
			createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
			streamIndex := 0
			engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
				streamIndex++
				if streamIndex == 1 {
					return io.NopCloser(strings.NewReader(test.line)), nil
				}
				return io.NopCloser(strings.NewReader("")), nil
			}
			client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
				return true, tailTestPod(podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}), nil
			})

			reader, err := engine.tailLogs(t.Context(), step, podName)
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, test.line, string(got))
		})
	}
}

func TestCopyTimestampedLogsReturnsWhenOutputReaderCloses(t *testing.T) {
	reader, writer := io.Pipe()
	require.NoError(t, reader.Close())
	defer writer.Close()

	engine := &kube{}
	last := time.Time{}
	skipping := false
	err := engine.copyTimestampedLogs(t.Context(), writer, strings.NewReader("2026-11-05T12:05:00.100Z line\n"), &last, &skipping)
	var pipeWriteErr *podLogPipeWriteError
	assert.ErrorAs(t, err, &pipeWriteErr)
	assert.ErrorIs(t, err, io.ErrClosedPipe)
}

func createTailTestPod(t *testing.T, client *fake.Clientset, name string, state kube_core_v1.ContainerState) {
	t.Helper()
	_, err := client.CoreV1().Pods("test-ns").Create(t.Context(), tailTestPod(name, state), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
}

func tailTestPod(name string, state kube_core_v1.ContainerState) *kube_core_v1.Pod {
	return &kube_core_v1.Pod{
		ObjectMeta: kube_meta_v1.ObjectMeta{Name: name, Namespace: "test-ns"},
		Status: kube_core_v1.PodStatus{ContainerStatuses: []kube_core_v1.ContainerStatus{{
			Name:  name,
			State: state,
		}}},
	}
}
