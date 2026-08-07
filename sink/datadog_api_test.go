package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip/v2"
)

type rejectThenFailSubmitter struct {
	calls int
}

type payloadLimitSubmitter struct {
	maxCompressed   int
	maxDecompressed int
	requests        int
	rejected        int
	acceptedSeries  []datadogV2.MetricSeries
}

func (s *payloadLimitSubmitter) Submit(_ context.Context, payload preparedDatadogPayload) (datadogSubmitResult, error) {
	s.requests++
	if payload.compressedBytes > s.maxCompressed || payload.uncompressedBytes > s.maxDecompressed {
		s.rejected++
		return datadogSubmitResult{statusCode: http.StatusRequestEntityTooLarge}, errors.New("payload too large")
	}

	decoded, err := decodePreparedMetricPayload(payload)
	if err != nil {
		return datadogSubmitResult{}, err
	}
	s.acceptedSeries = append(s.acceptedSeries, decoded.Series...)
	return datadogSubmitResult{statusCode: http.StatusAccepted}, nil
}

type checkpointFailureSubmitter struct {
	calls         int
	acceptedNames map[string]int
	acceptedBody  map[string]int
}

func (s *checkpointFailureSubmitter) Submit(_ context.Context, payload preparedDatadogPayload) (datadogSubmitResult, error) {
	s.calls++
	if s.calls == 2 {
		return datadogSubmitResult{}, errors.New("injected checkpoint failure")
	}

	decoded, err := decodePreparedMetricPayload(payload)
	if err != nil {
		return datadogSubmitResult{}, err
	}
	if s.acceptedNames == nil {
		s.acceptedNames = map[string]int{}
		s.acceptedBody = map[string]int{}
	}
	for _, series := range decoded.Series {
		s.acceptedNames[series.Metric]++
	}
	s.acceptedBody[string(payload.body)]++
	return datadogSubmitResult{statusCode: http.StatusAccepted}, nil
}

func decodePreparedMetricPayload(payload preparedDatadogPayload) (datadogV2.MetricPayload, error) {
	body := payload.body
	if payload.compressed {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return datadogV2.MetricPayload{}, err
		}
		body, err = io.ReadAll(reader)
		if err != nil {
			return datadogV2.MetricPayload{}, err
		}
		if err := reader.Close(); err != nil {
			return datadogV2.MetricPayload{}, err
		}
	}

	var decoded datadogV2.MetricPayload
	if err := json.Unmarshal(body, &decoded); err != nil {
		return datadogV2.MetricPayload{}, err
	}
	return decoded, nil
}

func testMetricSeries(count int) []datadogV2.MetricSeries {
	series := make([]datadogV2.MetricSeries, count)
	for i := range series {
		series[i] = datadogV2.MetricSeries{
			Metric: fmt.Sprintf("mysql.test.metric_%04d", i),
			Type:   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
			Points: []datadogV2.MetricPoint{{
				Timestamp: datadog.PtrInt64(1_700_000_000),
				Value:     datadog.PtrFloat64(float64(i)),
			}},
			Tags: []string{
				fmt.Sprintf("table:table_%04d", i),
				"description:" + strings.Repeat("x", 100),
			},
		}
	}
	return series
}

func testMetricsMetadata() *blip.Metrics {
	return &blip.Metrics{MonitorId: "test", Plan: "test", Level: "test", Interval: 1}
}

func newTestDatadogSender(submitter datadogMetricSubmitter) *Datadog {
	sender := &Datadog{
		monitorId:     "test",
		compress:      true,
		payloadLimits: defaultDatadogPayloadLimits(),
		submitter:     submitter,
	}
	sender.maxSeriesPerRequest.Store(int64(sender.payloadLimits.maxSeries))
	return sender
}

func (s *rejectThenFailSubmitter) Submit(context.Context, preparedDatadogPayload) (datadogSubmitResult, error) {
	s.calls++
	if s.calls == 1 {
		return datadogSubmitResult{statusCode: http.StatusRequestEntityTooLarge}, errors.New("payload too large")
	}
	return datadogSubmitResult{}, errors.New("network failure")
}

func TestPrepareDatadogPayloadHonorsCompressedLimit(t *testing.T) {
	series := testMetricSeries(1_000)
	limits := datadogPayloadLimits{
		maxCompressed:      1_000,
		maxDecompressed:    1_000_000,
		targetCompressed:   900,
		targetDecompressed: 900_000,
		maxSeries:          len(series),
	}

	payload, end, err := prepareDatadogPayload(context.Background(), series, 0, len(series), true, limits)
	require.NoError(t, err)
	require.Less(t, end, len(series), "compressed byte budget should split the input")
	require.LessOrEqual(t, payload.compressedBytes, limits.targetCompressed)
	require.LessOrEqual(t, payload.uncompressedBytes, limits.targetDecompressed)

	reader, err := gzip.NewReader(bytes.NewReader(payload.body))
	require.NoError(t, err)
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	var decoded datadogV2.MetricPayload
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded.Series, payload.seriesCount)
	require.Equal(t, series[:end], decoded.Series)
}

func TestDatadog413FallbackHalvesAndPersistsLimit(t *testing.T) {
	series := testMetricSeries(1_000)
	intake := &payloadLimitSubmitter{
		maxCompressed:   datadogMaxCompressedPayloadSize,
		maxDecompressed: 80_000,
	}
	// Simulate an effective server limit lower than the documented limit. The
	// locally valid first payload should receive 413 and trigger bounded halving.
	sender := newTestDatadogSender(intake)
	require.NoError(t, sender.sendAPI(context.Background(), series, testMetricsMetadata()))

	require.Greater(t, intake.rejected, 0)
	require.Len(t, intake.acceptedSeries, len(series))
	require.Less(t, sender.maxSeriesPerRequest.Load(), int64(len(series)))
	require.Greater(t, intake.requests, intake.rejected)
}

func TestDatadog413LimitSurvivesLaterFailure(t *testing.T) {
	series := testMetricSeries(10)
	submitter := &rejectThenFailSubmitter{}
	sender := &Datadog{
		compress:      true,
		payloadLimits: defaultDatadogPayloadLimits(),
		submitter:     submitter,
	}
	sender.maxSeriesPerRequest.Store(int64(len(series)))

	err := sender.sendAPI(context.Background(), series, testMetricsMetadata())
	require.ErrorContains(t, err, "network failure")
	require.Equal(t, 2, submitter.calls)
	require.Equal(t, int64(len(series)/2), sender.maxSeriesPerRequest.Load())
}

func TestPrepareDatadogPayloadStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	series := testMetricSeries(10)
	_, _, err := prepareDatadogPayload(ctx, series, 0, 10, true, defaultDatadogPayloadLimits())
	require.ErrorIs(t, err, context.Canceled)
}

func TestDatadogRetryCheckpointDoesNotResendAcknowledgedPayload(t *testing.T) {
	const metricCount = 40
	submitter := &checkpointFailureSubmitter{}
	sender := &Datadog{
		monitorId: "checkpoint-test",
		compress:  false,
		payloadLimits: datadogPayloadLimits{
			maxCompressed:      1_500,
			maxDecompressed:    10_000,
			targetCompressed:   1_200,
			targetDecompressed: 9_000,
			maxSeries:          metricCount,
		},
		submitter: submitter,
	}
	sender.maxSeriesPerRequest.Store(metricCount)
	retry := NewRetry(RetryArgs{
		MonitorId:     "checkpoint-test",
		Sink:          sender,
		BufferSize:    2,
		SendTimeout:   5 * time.Second,
		SendRetryWait: time.Millisecond,
	})

	require.NoError(t, retry.Send(context.Background(), getBlipMetrics(metricCount, blip.GAUGE, 1, false)))
	require.Greater(t, submitter.calls, 3, "test data must span at least three chunks")
	require.Equal(t, metricCount, len(submitter.acceptedNames))
	for name, count := range submitter.acceptedNames {
		require.Equalf(t, 1, count, "acknowledged metric %s was submitted more than once", name)
	}
	for body, count := range submitter.acceptedBody {
		require.Equalf(t, 1, count, "acknowledged payload was submitted more than once: %s", body)
	}
	require.Equal(t, -1, retry.top, "successful resume should remove the checkpointed queue entry")
}
