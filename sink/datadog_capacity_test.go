package sink

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test/mock"
	"github.com/stretchr/testify/require"
)

var datadogCapacityCounts = []int{0, 1, 10, 61, 383, 1000, 9999, 10000, 10001, 100000}

func capacityMetrics(n int) *blip.Metrics {
	m := getBlipMetrics(n, blip.GAUGE, 1, false)
	m.Begin = time.Unix(1700000000, 0)
	return m
}

// Exercise the real APIClient.CallAPI body copy without decoding or retaining
// requests in the measured path. HTTP transport costs are measured separately.
func capacityAPISender() *Datadog {
	cfg := datadog.NewConfiguration()
	cfg.HTTPClient = &http.Client{Transport: &mock.Transport{
		RoundTripFunc: func(r *http.Request) (*http.Response, error) {
			_, err := io.Copy(io.Discard, r.Body)
			r.Body.Close()
			return &http.Response{StatusCode: http.StatusAccepted,
				Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"errors":[]}`))}, err
		},
	}}
	return newTestDatadogSender(&datadogAPISubmitter{client: datadog.NewAPIClient(cfg), apiKey: "test"})
}

func BenchmarkDatadogCapacity(b *testing.B) {
	for _, n := range datadogCapacityCounts {
		b.Run(fmt.Sprintf("collect/%d", n), func(b *testing.B) {
			m, s := capacityMetrics(n), newTestDatadogSender(nil)
			state, _ := newDatadogSendCheckpoint(m, nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _, _, err := s.collectDatadogSeries(context.Background(), m, state.domains, datadogMetricCursor{}, datadogMaxSeriesPerPayload)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("send/%d", n), func(b *testing.B) {
			m, s := capacityMetrics(n), capacityAPISender()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				checkpoint, err := s.SendWithCheckpoint(context.Background(), m, nil)
				if err != nil || checkpoint != nil {
					b.Fatalf("send: %v, %v", checkpoint, err)
				}
			}
		})
	}
}

func BenchmarkDatadogCapacityLongTags(b *testing.B) {
	for _, n := range []int{1, 61, 1000} {
		for _, compressible := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/compressible=%t", n, compressible), func(b *testing.B) {
				m, s := capacityMetrics(n), capacityAPISender()
				for i := range m.Values["testdomain"] {
					tag := strings.Repeat("x", 4096)
					if !compressible {
						var text strings.Builder
						for j := 0; j < 64; j++ {
							fmt.Fprintf(&text, "%x", sha256.Sum256([]byte(fmt.Sprintf("%d/%d", i, j))))
						}
						tag = text.String()
					}
					m.Values["testdomain"][i].Group = map[string]string{"table": tag}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					checkpoint, err := s.SendWithCheckpoint(context.Background(), m, nil)
					if err != nil || checkpoint != nil {
						b.Fatalf("send: %v, %v", checkpoint, err)
					}
				}
			})
		}
	}
}

func TestDatadogCapacityWindows(t *testing.T) {
	for _, n := range datadogCapacityCounts {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			m, s := capacityMetrics(n), newTestDatadogSender(nil)
			state, err := newDatadogSendCheckpoint(m, nil)
			require.NoError(t, err)
			cursor, total := datadogMetricCursor{}, 0
			for {
				series, cursors, next, done, err := s.collectDatadogSeries(context.Background(), m, state.domains, cursor, datadogMaxSeriesPerPayload)
				require.NoError(t, err)
				require.LessOrEqual(t, len(series), datadogMaxSeriesPerPayload)
				require.Len(t, cursors, len(series))
				for i, v := range series {
					require.Equal(t, fmt.Sprintf("testdomain.testmetric%d", total+i+1), v.Metric)
				}
				total += len(series)
				if done {
					break
				}
				require.NotEqual(t, cursor, next)
				cursor = next
			}
			require.Equal(t, n, total)
		})
	}
}

func TestDatadogCapacitySkippedValuesAndCursor(t *testing.T) {
	m := capacityMetrics(6)
	v := m.Values["testdomain"]
	v[1].Type = blip.UNKNOWN
	v[2].Meta = map[string]string{"ts": "invalid"}
	m.Values = map[string][]blip.MetricValue{"a": v[:3], "b": nil, "c": v[3:]}
	s := newTestDatadogSender(nil)
	series, cursors, next, done, err := s.collectDatadogSeries(context.Background(), m, []string{"a", "b", "c"}, datadogMetricCursor{metric: 1}, 2)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, series, 2)
	require.Equal(t, "c.testmetric4", series[0].Metric)
	require.Equal(t, []datadogMetricCursor{{domain: 2, metric: 1}, {domain: 2, metric: 2}}, cursors)
	require.Equal(t, cursors[1], next)
	series, _, next, done, err = s.collectDatadogSeries(context.Background(), m, []string{"a", "b", "c"}, next, 10000)
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, series, 1)
	require.Equal(t, "c.testmetric6", series[0].Metric)
	require.Equal(t, datadogMetricCursor{domain: 3}, next)
}

func TestDatadogSmallWindowCapacity(t *testing.T) {
	m, s := capacityMetrics(61), newTestDatadogSender(nil)
	for _, offset := range []int{0, 60, 61} {
		series, cursors, _, done, err := s.collectDatadogSeries(context.Background(), m, []string{"testdomain"}, datadogMetricCursor{metric: offset}, datadogMaxSeriesPerPayload)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, 61-offset, cap(series))
		require.Equal(t, 61-offset, cap(cursors))
	}
}

func TestDatadogCapacityPayloadGrowthAndStart(t *testing.T) {
	series := testMetricSeries(3)
	series[1].Metric = strings.Repeat("long_name", 1024)
	series[1].Tags = []string{strings.Repeat("long_tag", 1024)}
	for _, compress := range []bool{false, true} {
		payload, end, err := prepareDatadogPayload(context.Background(), series, 1, 10000, compress, defaultDatadogPayloadLimits())
		require.NoError(t, err)
		require.Equal(t, 3, end)
		decoded, err := decodePreparedMetricPayload(payload)
		require.NoError(t, err)
		require.Equal(t, series[1:], decoded.Series)
	}
}

type capacitySubmitFunc func(context.Context, preparedDatadogPayload) (datadogSubmitResult, error)

func (f capacitySubmitFunc) Submit(ctx context.Context, p preparedDatadogPayload) (datadogSubmitResult, error) {
	return f(ctx, p)
}

func TestDatadogCapacityStreaming413AndResume(t *testing.T) {
	for _, failure := range []error{errors.New("network failure"), context.DeadlineExceeded, context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			var attempted []int
			var accepted []string
			failed := false
			s := newTestDatadogSender(capacitySubmitFunc(func(_ context.Context, p preparedDatadogPayload) (datadogSubmitResult, error) {
				attempted = append(attempted, p.seriesCount)
				if p.seriesCount > 2 {
					return datadogSubmitResult{statusCode: 413}, errors.New("too large")
				}
				if len(accepted) == 2 && !failed {
					failed = true
					return datadogSubmitResult{}, failure
				}
				decoded, err := decodePreparedMetricPayload(p)
				require.NoError(t, err)
				for _, v := range decoded.Series {
					accepted = append(accepted, v.Metric)
				}
				// Intake errors on a success still acknowledge this chunk.
				return datadogSubmitResult{statusCode: 202, errors: []string{"test intake warning"}}, nil
			}))
			m := capacityMetrics(10)
			checkpoint, err := s.SendWithCheckpoint(context.Background(), m, nil)
			require.ErrorIs(t, err, failure)
			require.NotNil(t, checkpoint)
			require.Equal(t, []int{10, 5, 2, 2}, attempted)
			require.Equal(t, int64(2), s.maxSeriesPerRequest.Load())
			checkpoint, err = s.SendWithCheckpoint(context.Background(), m, checkpoint)
			require.NoError(t, err)
			require.Nil(t, checkpoint)
			require.Len(t, accepted, 10)
			for i, name := range accepted {
				require.Equal(t, fmt.Sprintf("testdomain.testmetric%d", i+1), name)
			}
			attempted = nil
			_, err = s.SendWithCheckpoint(context.Background(), capacityMetrics(3), nil)
			require.NoError(t, err)
			require.Equal(t, []int{2, 1}, attempted)
		})
	}
}

func TestDatadogCapacityStreaming413Exhaustion(t *testing.T) {
	for _, n := range []int{1, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			calls := 0
			s := newTestDatadogSender(capacitySubmitFunc(func(context.Context, preparedDatadogPayload) (datadogSubmitResult, error) {
				calls++
				return datadogSubmitResult{statusCode: 413}, errors.New("too large")
			}))
			checkpoint, err := s.SendWithCheckpoint(context.Background(), capacityMetrics(n), nil)
			require.Error(t, err)
			state := checkpoint.(*datadogSendCheckpoint)
			require.Zero(t, state.sentSeries)
			require.Equal(t, datadogMetricCursor{}, state.cursor)
			require.Equal(t, min(n, datadogMax413Retries+1), calls)
		})
	}
}

func TestDatadogCapacityExactByteLimits(t *testing.T) {
	series := testMetricSeries(1)
	for _, compress := range []bool{false, true} {
		p, _, err := prepareDatadogPayload(context.Background(), series, 0, 10000, compress, defaultDatadogPayloadLimits())
		require.NoError(t, err)
		for _, delta := range []int{-1, 0, 1} {
			limits := defaultDatadogPayloadLimits()
			limits.maxCompressed = p.compressedBytes + delta
			limits.targetCompressed = limits.maxCompressed
			limits.maxDecompressed = p.uncompressedBytes + delta
			limits.targetDecompressed = limits.maxDecompressed
			_, _, err := prepareDatadogPayload(context.Background(), series, 0, 10000, compress, limits)
			if delta < 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		}
	}
}

func TestDatadogCapacityConcurrentSend(t *testing.T) {
	s := capacityAPISender()
	m := capacityMetrics(61)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				checkpoint, err := s.SendWithCheckpoint(context.Background(), m, nil)
				if err != nil || checkpoint != nil {
					t.Errorf("send: %v, %v", checkpoint, err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestDatadogCapacityCheckpointDuringQueueOverflow(t *testing.T) {
	blocked, release := make(chan struct{}), make(chan struct{})
	var accepted []string
	calls := 0
	s := newTestDatadogSender(capacitySubmitFunc(func(_ context.Context, p preparedDatadogPayload) (datadogSubmitResult, error) {
		calls++
		if calls == 2 {
			close(blocked)
			<-release
			return datadogSubmitResult{}, errors.New("in-flight failure")
		}
		decoded, err := decodePreparedMetricPayload(p)
		if err != nil {
			return datadogSubmitResult{}, err
		}
		for _, v := range decoded.Series {
			accepted = append(accepted, v.Metric)
		}
		return datadogSubmitResult{statusCode: 202}, nil
	}))
	s.maxSeriesPerRequest.Store(2)
	retry := NewRetry(RetryArgs{MonitorId: "capacity-overflow", Sink: s, BufferSize: 60, SendTimeout: 10 * time.Second, SendRetryWait: time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- retry.Send(context.Background(), capacityMetrics(10)) }()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("send did not reach second chunk")
	}
	// Enqueuing 61 entries evicts the in-flight item and the first new item.
	// Its acknowledged prefix must stay accepted, while Retry keeps its normal
	// newest-first overflow behavior for the remaining queue.
	for i := 0; i < 61; i++ {
		m := capacityMetrics(1)
		m.Values["testdomain"][0].Name = fmt.Sprintf("queued_%d", i)
		require.NoError(t, retry.Send(context.Background(), m))
	}
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("retry did not drain")
	}
	require.Len(t, accepted, 62)
	require.Equal(t, []string{"testdomain.testmetric1", "testdomain.testmetric2"}, accepted[:2])
	for i := 0; i < 60; i++ {
		require.Equal(t, fmt.Sprintf("testdomain.queued_%d", 60-i), accepted[i+2])
	}
	require.Equal(t, -1, retry.top)
}
