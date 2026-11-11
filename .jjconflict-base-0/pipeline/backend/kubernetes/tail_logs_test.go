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
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
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

func TestTailLogsStopsWhenReopenedLogStreamIsNotFound(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-reopen-not-found")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})

	var openCount atomic.Int32
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		if openCount.Add(1) == 1 {
			return io.NopCloser(strings.NewReader("")), nil
		}
		return nil, kube_errors.NewNotFound(kube_core_v1.Resource("pods"), podName)
	}
	var getCount atomic.Int32
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		getCount.Add(1)
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}), nil
	})
	var waitCount atomic.Int32
	engine.waitLogReopen = func(ctx context.Context, _ time.Duration) error {
		if waitCount.Add(1) == 1 {
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	reader, err := engine.tailLogs(ctx, step, podName)
	require.NoError(t, err)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.Copy(io.Discard, reader)
		readDone <- readErr
	}()
	select {
	case err := <-readDone:
		assert.NoError(t, err)
		assert.Equal(t, int32(2), openCount.Load())
		assert.Equal(t, int32(1), getCount.Load())
	case <-time.After(time.Second):
		t.Fatal("reader did not reach EOF after the reopened log stream returned NotFound")
	}
	require.NoError(t, reader.Close())
}

func TestTailLogsDrainsFinalStreamAfterReopenFailure(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-final-after-reopen-failure")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})

	var openCount atomic.Int32
	engine.openLogStream = func(_ context.Context, _, _ string, opts *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		switch openCount.Add(1) {
		case 1:
			return io.NopCloser(strings.NewReader("2026-11-05T12:11:00.123Z first line\n")), nil
		case 2, 3:
			return nil, errors.New("temporary log stream failure")
		default:
			if opts.Follow {
				return nil, errors.New("final log stream must not follow")
			}
			return io.NopCloser(strings.NewReader("2026-11-05T12:11:01.123Z second line\n2026-11-05T12:11:02.123Z third line\n")), nil
		}
	}
	var getCount atomic.Int32
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		state := kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}
		if getCount.Add(1) == 2 {
			state = kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}
		}
		return true, tailTestPod(podName, state), nil
	})
	engine.waitLogReopen = func(context.Context, time.Duration) error { return nil }

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "first line\nsecond line\nthird line\n", string(got))
	assert.Equal(t, int32(4), openCount.Load())
	assert.Equal(t, int32(3), getCount.Load())
}

func TestTailLogsStopsFinalDrainAfterRetryDeadline(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-final-drain-deadline")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}})

	var openCount atomic.Int32
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		if openCount.Add(1) == 1 {
			return io.NopCloser(strings.NewReader("2026-11-05T12:12:00.123Z initial line\n")), nil
		}
		return nil, errors.New("temporary final log stream failure")
	}
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}), nil
	})
	startTime := time.Unix(1_800_000_000, 0)
	var currentTime atomic.Int64
	currentTime.Store(startTime.UnixNano())
	engine.logRetryTimeout = 500 * time.Millisecond
	engine.logRetryNow = func() time.Time { return time.Unix(0, currentTime.Load()) }
	engine.waitLogReopen = func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		currentTime.Add(int64(delay))
		return nil
	}

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	finishedAt := time.Unix(0, currentTime.Load())
	require.NoError(t, reader.Close())
	assert.Equal(t, "initial line\n", string(got))
	assert.Greater(t, openCount.Load(), int32(1))
	assert.LessOrEqual(t, finishedAt.Sub(startTime), 500*time.Millisecond)
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
	atLast := 0
	skipping := false
	skipAtLast := 0
	delivered := false
	err := engine.copyTimestampedLogs(t.Context(), writer, strings.NewReader("2026-11-05T12:05:00.100Z line\n"), &last, &atLast, &skipping, &skipAtLast, &delivered)
	var pipeWriteErr *podLogPipeWriteError
	assert.ErrorAs(t, err, &pipeWriteErr)
	assert.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestTailLogsRecoversEqualTimestampLinesAfterBoundary(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-equal-boundary")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	line1 := "2026-11-05T12:06:00.123456789Z first\n"
	line2 := "2026-11-05T12:06:00.123456789Z second\n"
	streams := []string{line1, line1 + line2, line1 + line2}
	var opened []*kube_core_v1.PodLogOptions
	engine.openLogStream = func(_ context.Context, _, _ string, opts *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		opened = append(opened, opts.DeepCopy())
		return io.NopCloser(strings.NewReader(streams[len(opened)-1])), nil
	}
	getCount := 0
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		getCount++
		state := kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}
		if getCount == 2 {
			state = kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}
		}
		return true, tailTestPod(podName, state), nil
	})

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "first\nsecond\n", string(got))
	assert.Len(t, opened, 3)
}

func TestTailLogsStreamsLargeLineWithFixedBuffer(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-large-line")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	body := strings.Repeat("x", 5<<20)
	line := "2026-11-05T12:07:00.123456789Z " + body
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(line)), nil
	}
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}), nil
	})

	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Len(t, got, len(body))
	assert.Equal(t, sha256.Sum256([]byte(body)), sha256.Sum256(got))
	assert.Equal(t, logReadBufferSize, newPodLogReader(strings.NewReader(line)).Size())
}

func TestTailLogsReopensAfterMoreThanTwentyEarlyEOFs(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-many-reopens")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	const emptyStreams = 21
	var openCount atomic.Int32
	var openFailures atomic.Int32
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		count := openCount.Add(1)
		if count <= emptyStreams {
			return io.NopCloser(strings.NewReader("")), nil
		}
		if count <= emptyStreams+2 {
			openFailures.Add(1)
			return nil, errors.New("temporary stream open failure")
		}
		return io.NopCloser(strings.NewReader("2026-11-05T12:08:00.123456789Z recovered\n")), nil
	}
	var waits []time.Duration
	engine.waitLogReopen = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	getCount := atomic.Int32{}
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		count := getCount.Add(1)
		state := kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}
		if count == emptyStreams+1 {
			state = kube_core_v1.ContainerState{Terminated: &kube_core_v1.ContainerStateTerminated{ExitCode: 0}}
		}
		return true, tailTestPod(podName, state), nil
	})
	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "recovered\n", string(got))
	assert.Greater(t, openCount.Load(), int32(emptyStreams))
	assert.Equal(t, int32(2), openFailures.Load())
	assert.Len(t, waits, emptyStreams+1)
	assert.Equal(t, initialLogReopenDelay, waits[0])
	assert.Equal(t, maxLogReopenDelay, waits[emptyStreams-1])
	assert.Equal(t, maxLogReopenDelay, waits[emptyStreams])
	assert.Equal(t, maxLogReopenDelay, waits[len(waits)-1])
}

func TestTailLogsReaderCloseClosesSourceAndStopsTailGoroutine(t *testing.T) {
	client := fake.NewClientset()
	engine := makeEngine(client)
	step := makeStep("tail-close-source")
	podName, err := stepToPodName(step)
	require.NoError(t, err)
	createTailTestPod(t, client, podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}})
	source := &blockingLogStream{closed: make(chan struct{})}
	opened := make(chan struct{})
	engine.openLogStream = func(_ context.Context, _, _ string, _ *kube_core_v1.PodLogOptions) (io.ReadCloser, error) {
		close(opened)
		return source, nil
	}
	var getCount atomic.Int32
	client.PrependReactor("get", "pods", func(kube_testing.Action) (bool, kube_runtime.Object, error) {
		getCount.Add(1)
		return true, tailTestPod(podName, kube_core_v1.ContainerState{Running: &kube_core_v1.ContainerStateRunning{}}), nil
	})
	reader, err := engine.tailLogs(t.Context(), step, podName)
	require.NoError(t, err)
	<-opened
	require.NoError(t, reader.Close())
	select {
	case <-source.closed:
	default:
		t.Fatal("closing the returned reader did not close the source stream")
	}
	assert.Zero(t, getCount.Load(), "closed reader must stop before pod lookup or reopen")
}

type blockingLogStream struct {
	closed chan struct{}
	once   sync.Once
}

func (s *blockingLogStream) Read([]byte) (int, error) {
	<-s.closed
	return 0, io.EOF
}

func (s *blockingLogStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func createTailTestPod(t *testing.T, client *fake.Clientset, name string, state kube_core_v1.ContainerState) {
	t.Helper()
	_, err := client.CoreV1().Pods("test-ns").Create(t.Context(), tailTestPod(name, state), kube_meta_v1.CreateOptions{})
	require.NoError(t, err)
}

func tailTestPod(name string, state kube_core_v1.ContainerState) *kube_core_v1.Pod {
	metadata := kube_meta_v1.ObjectMeta{Name: name, Namespace: "test-ns"}
	return &kube_core_v1.Pod{
		ObjectMeta: metadata,
		Status: kube_core_v1.PodStatus{ContainerStatuses: []kube_core_v1.ContainerStatus{{
			Name:  name,
			State: state,
		}}},
	}
}

func BenchmarkCopyTimestampedLogs(b *testing.B) {
	const lineCount = 10_000
	const line = "2026-11-05T12:10:00.123456789Z benchmark line\n"
	logs := strings.Repeat(line, lineCount)
	engine := &kube{}
	b.ReportAllocs()
	b.SetBytes(int64(len(logs)))

	for b.Loop() {
		reader, writer := io.Pipe()
		readDone := make(chan error, 1)
		go func() {
			_, readErr := io.Copy(io.Discard, reader)
			closeErr := reader.Close()
			readDone <- errors.Join(readErr, closeErr)
		}()
		last := time.Time{}
		atLast := 0
		skipping := false
		skipAtLast := 0
		delivered := false
		copyErr := engine.copyTimestampedLogs(context.Background(), writer, strings.NewReader(logs), &last, &atLast, &skipping, &skipAtLast, &delivered)
		if !errors.Is(copyErr, io.EOF) {
			b.Fatalf("copyTimestampedLogs() error = %v, want EOF", copyErr)
		}
		if closeErr := writer.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
		if readErr := <-readDone; readErr != nil {
			b.Fatal(readErr)
		}
	}
}
