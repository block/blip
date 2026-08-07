package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test/mock"
	"github.com/go-test/deep"
)

func defaultOps() map[string]string {
	return map[string]string{
		"api-key-auth": "testkey",
		"app-key-auth": "testkey",
		"api-compress": "true",
	}
}

func okHttpClient() *http.Client {
	return &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			},
		},
	}
}

func getBlipCounterMetrics(valuesCount int, metricValue float64, generateDelta bool) *blip.Metrics {
	values := make([]blip.MetricValue, 0, valuesCount)

	for i := 0; i < valuesCount; i++ {
		metricType := blip.UNKNOWN
		if generateDelta && i%2 == 0 {
			metricType = blip.DELTA_COUNTER
		} else {
			metricType = blip.CUMULATIVE_COUNTER
		}
		values = append(values, blip.MetricValue{
			Name:  fmt.Sprintf("testmetric%d", i+1),
			Value: metricValue,
			Type:  metricType,
		})
	}

	return &blip.Metrics{
		Begin:     time.Now().Add(-1 * time.Hour),
		End:       time.Now(),
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"testdomain": values,
		},
	}
}

func getBlipMetrics(valuesCount int, metricType byte, metricValue float64, generateDelta bool) *blip.Metrics {
	values := make([]blip.MetricValue, 0, valuesCount)

	for i := 0; i < valuesCount; i++ {
		values = append(values, blip.MetricValue{
			Name:  fmt.Sprintf("testmetric%d", i+1),
			Value: metricValue,
			Type:  metricType,
		})
	}

	return &blip.Metrics{
		Begin:     time.Now().Add(-1 * time.Hour),
		End:       time.Now(),
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"testdomain": values,
		},
	}
}

func getPayloadSize(r *http.Request) (int, error) {
	bodyReader, err := r.GetBody()
	if err != nil {
		return 0, err
	}

	defer bodyReader.Close()
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return 0, err
	}

	return len(body), nil
}

func TestDatadogSink(t *testing.T) {
	ddSink, err := NewDatadog("testmonitor", defaultOps(), map[string]string{}, okHttpClient())

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}

	err = ddSink.Send(context.Background(), getBlipMetrics(10, blip.GAUGE, 1.0, false))

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}
}

func TestDatadogSendBoundedByClientTimeout(t *testing.T) {
	httpClient := &http.Client{
		Timeout: 200 * time.Millisecond,
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				<-r.Context().Done()
				return nil, r.Context().Err()
			},
		},
	}
	ddSink, err := NewDatadog("testmonitor", defaultOps(), map[string]string{}, httpClient)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- ddSink.Send(context.Background(), getBlipCounterMetrics(1, 1.0, false))
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Datadog.Send stalled: http.Client.Timeout did not bound the request")
	}
}

func TestDatadogMetricsPerRequest(t *testing.T) {
	callCount := 0
	rejectedCount := 0
	maxBodySize := 0
	testPayloadSize := 5000
	metricCount := 100

	httpClient := &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				callCount++

				bodySize, err := getPayloadSize(r)
				if err != nil {
					return nil, err
				}
				if bodySize > maxBodySize {
					maxBodySize = bodySize
				}

				if bodySize > testPayloadSize {
					rejectedCount++
					return &http.Response{
						StatusCode: http.StatusRequestEntityTooLarge,
					}, nil
				}

				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			},
		},
	}

	ops := defaultOps()
	ops["api-compress"] = "false" // Turn off compression so that we get easier calculations for sizing
	ddSink, err := NewDatadog("testmonitor", ops, map[string]string{}, httpClient)
	require.NoError(t, err)
	ddSink.payloadLimits.maxCompressed = testPayloadSize
	ddSink.payloadLimits.targetCompressed = testPayloadSize * 9 / 10

	err = ddSink.Send(context.Background(), getBlipMetrics(metricCount, blip.GAUGE, 1.0, false))

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}

	require.Greater(t, callCount, 1)
	require.Zero(t, rejectedCount, "locally-sized requests should not receive HTTP 413")
	require.LessOrEqual(t, maxBodySize, testPayloadSize)
}

func TestDatadogMetricsPerRequestWithCompression(t *testing.T) {
	callCount := 0
	rejectedCount := 0
	maxBodySize := 0
	testPayloadSize := 1500
	metricCount := 600

	httpClient := &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				callCount++

				bodySize, err := getPayloadSize(r)
				if err != nil {
					return nil, err
				}

				if bodySize > maxBodySize {
					maxBodySize = bodySize
				}
				if bodySize > testPayloadSize {
					rejectedCount++
					return &http.Response{
						StatusCode: http.StatusRequestEntityTooLarge,
					}, nil
				}

				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			},
		},
	}

	ops := defaultOps()
	ddSink, err := NewDatadog("testmonitor", ops, map[string]string{}, httpClient)
	require.NoError(t, err)
	ddSink.payloadLimits.maxCompressed = testPayloadSize
	ddSink.payloadLimits.targetCompressed = testPayloadSize * 9 / 10

	err = ddSink.Send(context.Background(), getBlipMetrics(metricCount, blip.GAUGE, 1.0, false))

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}

	require.Greater(t, callCount, 1)
	require.Zero(t, rejectedCount, "locally-sized requests should not receive HTTP 413")
	require.LessOrEqual(t, maxBodySize, testPayloadSize)
}

func TestDatadogMetricsPerRequestHeterogeneousSeries(t *testing.T) {
	callCount := 0
	testPayloadSize := 5000
	metricCount := 500
	trCount := 0
	var collectedMetrics []string

	httpClient := &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				callCount++

				bodySize, err := getPayloadSize(r)
				if err != nil {
					return nil, err
				}

				if bodySize > testPayloadSize {
					return &http.Response{
						StatusCode: http.StatusRequestEntityTooLarge,
					}, nil
				}

				var payload datadogV2.MetricPayload
				body, _ := r.GetBody()
				defer body.Close()
				data, _ := io.ReadAll(body)
				json.Unmarshal(data, &payload)

				for _, metric := range payload.Series {
					collectedMetrics = append(collectedMetrics, metric.Metric)
				}

				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			},
		},
	}

	ops := defaultOps()
	ops["api-compress"] = "false" // Turn off compression so that we get easier calculations for sizing
	ddSink, err := NewDatadog("testmonitor", ops, map[string]string{}, httpClient)
	require.NoError(t, err)
	ddSink.payloadLimits.maxCompressed = testPayloadSize
	ddSink.payloadLimits.targetCompressed = testPayloadSize * 9 / 10
	ddSink.tr = &mock.Tr{
		TranslateFunc: func(domain, metric string) string {
			trCount++
			// Once we get past a certain number of messages add a long prefix to force
			// the sink to recalculate the max metrics per request.
			if trCount > 250 {
				return fmt.Sprintf("%s.THEQUICKBROWNFOXJUMPEDOVERTHELAZYDOGMAKETHISVERYLONG%s", domain, metric)
			} else {
				return fmt.Sprintf("%s.%s", domain, metric)
			}
		},
	}

	blipMetrics := getBlipMetrics(metricCount, blip.GAUGE, 1.0, false)
	err = ddSink.Send(context.Background(), blipMetrics)

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}

	if callCount == 1 {
		t.Error("Expected more than 1 call but only got 1.")
	}

	expectedMetrics := make([]string, 0, len(blipMetrics.Values))
	expectedCount := 0
	for _, metric := range blipMetrics.Values["testdomain"] {
		expectedCount++
		if expectedCount > 250 {
			expectedMetrics = append(expectedMetrics, fmt.Sprintf("testdomain.THEQUICKBROWNFOXJUMPEDOVERTHELAZYDOGMAKETHISVERYLONG%s", metric.Name))
		} else {
			expectedMetrics = append(expectedMetrics, fmt.Sprintf("testdomain.%s", metric.Name))
		}

	}

	if diff := deep.Equal(expectedMetrics, collectedMetrics); diff != nil {
		t.Fatal(diff)
	}
}

func TestDatadogMetricsErrorResponseFromAPI(t *testing.T) {
	errors := []string{"validation error 1", "validation error 2"}
	resp := map[string][]string{
		"errors": errors,
	}
	respJSON, err := json.Marshal(resp)
	require.NoError(t, err)

	httpClient := &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Body:       io.NopCloser(bytes.NewReader(respJSON)),
				}, nil
			},
		},
	}

	ddSink, err := NewDatadog("testmonitor", defaultOps(), map[string]string{}, httpClient)
	require.NoError(t, err)

	err = ddSink.Send(context.Background(), getBlipMetrics(10, blip.GAUGE, 1.0, false))

	// validation errors should be logged and the sink should continue
	require.NoError(t, err)
}

func TestDatadogInDeltaSink(t *testing.T) {
	callCount := 0
	metricCount := 50
	trCount := 0
	collectedMetrics := map[string]float64{}

	httpClient := &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				callCount++

				var payload datadogV2.MetricPayload
				body, _ := r.GetBody()
				defer body.Close()
				data, _ := io.ReadAll(body)
				json.Unmarshal(data, &payload)

				for _, metric := range payload.Series {
					require.Equal(t, 1, len(metric.Points), "metric series should have only 1 metric")
					collectedMetrics[metric.Metric] = *metric.Points[0].Value
				}

				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			},
		},
	}

	ops := defaultOps()
	ops["api-compress"] = "false" // Turn off compression so that we get easier calculations for sizing
	ddSink, err := NewDatadog("testmonitor", ops, map[string]string{}, httpClient)
	require.NoError(t, err)
	ddSink.tr = &mock.Tr{
		TranslateFunc: func(domain, metric string) string {
			trCount++
			return fmt.Sprintf("%s.%s", domain, metric)
		},
	}

	deltaSink := NewDelta(ddSink)

	blipMetricsFirstBatch := getBlipCounterMetrics(metricCount, 10.0, true)
	err = deltaSink.Send(context.Background(), blipMetricsFirstBatch)
	require.NoError(t, err)
	// only half the metrics with Delta counter values should be sent
	require.Equal(t, metricCount/2, len(collectedMetrics))
	expectedMetrics := map[string]float64{}
	for _, metric := range blipMetricsFirstBatch.Values["testdomain"] {
		if metric.Type == blip.DELTA_COUNTER {
			name := fmt.Sprintf("testdomain.%s", metric.Name)
			expectedMetrics[name] = metric.Value
		}
	}
	// reset collected
	collectedMetrics = map[string]float64{}
	blipMetricsSecondBatch := getBlipCounterMetrics(metricCount, 20.0, true)
	err = deltaSink.Send(context.Background(), blipMetricsSecondBatch)
	require.NoError(t, err)

	expectedMetrics = getExpectedMetrics(blipMetricsSecondBatch, blipMetricsFirstBatch)
	if diff := deep.Equal(expectedMetrics, collectedMetrics); diff != nil {
		t.Fatal(diff)
	}

	// simulate a restart
	blipMetricsThirdBatch := getBlipCounterMetrics(metricCount, 1.0, true)
	err = deltaSink.Send(context.Background(), blipMetricsThirdBatch)
	require.Equal(t, metricCount, len(collectedMetrics))

	// reset collected
	collectedMetrics = map[string]float64{}

	// send final batch
	blipMetricsFourthBatch := getBlipCounterMetrics(metricCount, 5.0, true)
	err = deltaSink.Send(context.Background(), blipMetricsFourthBatch)
	require.NoError(t, err)

	expectedMetrics = getExpectedMetrics(blipMetricsFourthBatch, blipMetricsThirdBatch)
	if diff := deep.Equal(expectedMetrics, collectedMetrics); diff != nil {
		t.Fatal(diff)
	}
}

// This test demonstrates how the Delta sink can prevent issues with the Retry
// sink. The Delta sink replaces CUMULATIVE_COUNTER values in the metric batch
// with DELTA_COUNTER values, before sending the modified batch to the Retry sink.
// This causes any retires to use the already calculated delta values instead of
// causing a recaculation that uses bad prior metric values.
func TestDatadogInDeltaRetrySink(t *testing.T) {
	metricCount := 50
	collectedMetrics := map[string]float64{}

	// Create controls for handling the timing
	// of goroutines so we can coordinate the scenario
	var wg sync.WaitGroup
	failChan := make(chan bool, 2)
	enteredChan := make(chan struct{})

	httpClient := &http.Client{
		Transport: &mock.Transport{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				// Track when the submission is running
				wg.Add(1)
				defer wg.Done()

				// Wait for the caller to confirm that it knows that
				// the submission has started
				enteredChan <- struct{}{}

				// Determine if the submission should fail or not
				shouldFail := <-failChan
				if shouldFail {
					return nil, fmt.Errorf("Test Failure")
				}

				var payload datadogV2.MetricPayload
				body, _ := r.GetBody()
				defer body.Close()
				data, _ := io.ReadAll(body)
				json.Unmarshal(data, &payload)

				for _, metric := range payload.Series {
					require.Equal(t, 1, len(metric.Points), "metric series should have only 1 metric")
					collectedMetrics[metric.Metric] = *metric.Points[0].Value
				}

				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			},
		},
	}

	ops := defaultOps()
	ops["api-compress"] = "false" // Turn off compression so that we get easier calculations for sizing
	ddSink, err := NewDatadog("testmonitor", ops, map[string]string{}, httpClient)
	require.NoError(t, err)
	ddSink.tr = &mock.Tr{
		TranslateFunc: func(domain, metric string) string {
			return fmt.Sprintf("%s.%s", domain, metric)
		},
	}

	// Create a retry sink and wrap the datadog sink
	retrySink := NewRetry(RetryArgs{
		MonitorId:  "m1",
		Sink:       ddSink,
		BufferSize: 4,
	})

	// Wrap the retry sink in a delta sink
	deltaSink := NewDelta(retrySink)

	blipMetricsFirstBatch := getBlipCounterMetrics(metricCount, 10.0, true)

	// Don't fail on the first attempt. We want to get the first set of
	// data points established for the delta calculations
	failChan <- false

	// Running deltaSink.Send will block on the submission, so handle
	// waiting for the signal on the channel in the background
	go func() {
		<-enteredChan
	}()
	err = deltaSink.Send(context.Background(), blipMetricsFirstBatch)
	require.NoError(t, err)
	// only half the metrics with Delta counter values should be sent
	require.Equal(t, metricCount/2, len(collectedMetrics))

	// reset collected
	collectedMetrics = map[string]float64{}
	blipMetricsSecondBatch := getBlipCounterMetrics(metricCount, 20.0, true)

	// Simulate a metric failing to send, but wait until we have the
	// next metric queued to allow the failure to happen. This will
	// cause the retry queue to run the next iteration of the retry loop,
	// which will pull the *third* metric batch rather than trying the second again.
	// The order will be:
	// Try Second Batch -> FAIL
	// Try Third Batch -> SUCCESS
	// Try Second Batch (2nd attempt) -> SUCCESS

	// Launch the deltaSink.Send in a goroutine as it will block otherwise
	go func() {
		err = deltaSink.Send(context.Background(), blipMetricsSecondBatch)
		require.NoError(t, err)
	}()

	// Wait to confirm that the second attempt has started the submission.
	// We confirm that the goroutine has started so that the third submission
	// will not block, as retrySink inside the Delta sink will detect that
	// another routine is already processing and simply queue the third batch.
	<-enteredChan

	// Simulate a third metric coming in and queuing
	blipMetricsThirdBatch := getBlipCounterMetrics(metricCount, 40.0, true)
	err = deltaSink.Send(context.Background(), blipMetricsThirdBatch)

	// Fail the second batch
	failChan <- true
	// Confirm the third batch has entered submission
	<-enteredChan
	// Allow the third batch to process as expected
	failChan <- false

	// Confirm the second batch (2nd attempt) has entered submission
	<-enteredChan
	// Allow the second batch to process as expected
	failChan <- false

	// Wait for all batches to finish
	wg.Wait()

	// Since the second batch was processed last we expect that the collected metrics
	// will match the delta values between the second batch and the first batch
	// once the second batch was successfully submitted.
	expectedMetrics := map[string]float64{}
	expectedMetrics = getExpectedMetrics(blipMetricsSecondBatch, blipMetricsFirstBatch)
	if diff := deep.Equal(expectedMetrics, collectedMetrics); diff != nil {
		t.Fatal(diff)
	}

	require.Equal(t, metricCount, len(collectedMetrics))
	if diff := deep.Equal(expectedMetrics, collectedMetrics); diff != nil {
		t.Fatal(diff)
	}
}

func getExpectedMetrics(currentBatch, previousBatch *blip.Metrics) map[string]float64 {
	expectedMetrics := map[string]float64{}
	for i, currentMetric := range currentBatch.Values["testdomain"] {
		var metricVal float64
		if currentMetric.Type == blip.DELTA_COUNTER {
			metricVal = currentMetric.Value
		} else {
			metricVal = currentMetric.Value - previousBatch.Values["testdomain"][i].Value
		}
		name := fmt.Sprintf("testdomain.%s", currentMetric.Name)
		expectedMetrics[name] = metricVal
	}
	return expectedMetrics
}
