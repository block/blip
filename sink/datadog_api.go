// Copyright 2024 Block, Inc.

package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/status"
)

const (
	datadogMaxCompressedPayloadSize   = 512_000
	datadogMaxDecompressedPayloadSize = 5 * 1024 * 1024

	// Keep payloads below the documented hard limits. Exact byte measurement
	// makes the margin defensive rather than part of the sizing algorithm.
	datadogTargetCompressedPayloadSize   = datadogMaxCompressedPayloadSize * 9 / 10
	datadogTargetDecompressedPayloadSize = datadogMaxDecompressedPayloadSize * 9 / 10

	// This is a latency and CPU guard, not a payload-size estimate. Exact raw
	// and compressed byte counts determine whether a payload can be submitted.
	datadogMaxSeriesPerPayload = 10_000
	datadogMax413Retries       = 4
)

var (
	datadogPayloadPrefix = []byte(`{"series":[`)
	datadogPayloadSuffix = []byte(`]}`)
)

type datadogPayloadLimits struct {
	maxCompressed      int
	maxDecompressed    int
	targetCompressed   int
	targetDecompressed int
	maxSeries          int
}

func defaultDatadogPayloadLimits() datadogPayloadLimits {
	return datadogPayloadLimits{
		maxCompressed:      datadogMaxCompressedPayloadSize,
		maxDecompressed:    datadogMaxDecompressedPayloadSize,
		targetCompressed:   datadogTargetCompressedPayloadSize,
		targetDecompressed: datadogTargetDecompressedPayloadSize,
		maxSeries:          datadogMaxSeriesPerPayload,
	}
}

func (l datadogPayloadLimits) validate() error {
	if l.maxCompressed <= 0 || l.maxDecompressed <= 0 || l.maxSeries <= 0 {
		return fmt.Errorf("invalid Datadog payload limits: %+v", l)
	}
	if l.targetCompressed <= 0 || l.targetCompressed > l.maxCompressed {
		return fmt.Errorf("invalid Datadog compressed payload target: %d", l.targetCompressed)
	}
	if l.targetDecompressed <= 0 || l.targetDecompressed > l.maxDecompressed {
		return fmt.Errorf("invalid Datadog decompressed payload target: %d", l.targetDecompressed)
	}
	return nil
}

type preparedDatadogPayload struct {
	body              []byte
	seriesCount       int
	uncompressedBytes int
	compressedBytes   int
	compressed        bool
}

type datadogSubmitResult struct {
	statusCode int
	errors     []string
	body       string
}

type datadogMetricSubmitter interface {
	Submit(context.Context, preparedDatadogPayload) (datadogSubmitResult, error)
}

// datadogAPISubmitter submits a body that has already been encoded and sized.
// The generated Datadog SubmitMetrics method cannot accept a prepared body: it
// always marshals and compresses the MetricPayload itself.
type datadogAPISubmitter struct {
	client *datadog.APIClient
	apiKey string
}

func (s *datadogAPISubmitter) Submit(ctx context.Context, payload preparedDatadogPayload) (datadogSubmitResult, error) {
	var result datadogSubmitResult
	requestCtx := context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{
		"apiKeyAuth": {Key: s.apiKey},
	})

	baseURL, err := s.client.GetConfig().ServerURLWithContext(requestCtx, "v2.MetricsApi.SubmitMetrics")
	if err != nil {
		return result, err
	}

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v2/series", bytes.NewReader(payload.body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", s.apiKey)
	if payload.compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	cfg := s.client.GetConfig()
	for name, value := range cfg.DefaultHeader {
		req.Header.Set(name, value)
	}
	if cfg.UserAgent != "" {
		req.Header.Set("User-Agent", cfg.UserAgent)
	}

	resp, err := s.client.CallAPI(req)
	if resp != nil {
		result.statusCode = resp.StatusCode
	}
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(responseBody))
	result.body = string(responseBody)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return result, fmt.Errorf("HTTP status %d: %s", resp.StatusCode, strings.TrimSpace(result.body))
	}

	if len(bytes.TrimSpace(responseBody)) > 0 {
		var accepted datadogV2.IntakePayloadAccepted
		if err := json.Unmarshal(responseBody, &accepted); err != nil {
			return result, fmt.Errorf("decode Datadog response: %w", err)
		}
		result.errors = accepted.Errors
	}

	return result, nil
}

// prepareDatadogPayload constructs the exact JSON body used on the wire. It
// stops at both a decompressed byte budget and a defensive series-count guard,
// then validates the actual gzip size. The returned end index is exclusive.
func prepareDatadogPayload(ctx context.Context, series []datadogV2.MetricSeries, start, maxSeries int, compress bool, limits datadogPayloadLimits) (preparedDatadogPayload, int, error) {
	var prepared preparedDatadogPayload
	if err := limits.validate(); err != nil {
		return prepared, start, err
	}
	if start < 0 || start >= len(series) {
		return prepared, start, fmt.Errorf("invalid Datadog payload start index %d for %d series", start, len(series))
	}
	if maxSeries <= 0 || maxSeries > limits.maxSeries {
		maxSeries = limits.maxSeries
	}

	rawTarget := limits.targetCompressed
	rawHardLimit := limits.maxCompressed
	if compress {
		rawTarget = limits.targetDecompressed
		rawHardLimit = limits.maxDecompressed
	}

	var raw bytes.Buffer
	grow := rawTarget
	if estimated := maxSeries * 512; estimated < grow {
		grow = estimated
	}
	if grow > 0 {
		raw.Grow(grow)
	}
	raw.Write(datadogPayloadPrefix)

	offsets := make([]int, 0, maxSeries)
	end := start
	for end < len(series) && len(offsets) < maxSeries {
		select {
		case <-ctx.Done():
			return prepared, start, ctx.Err()
		default:
		}

		encoded, err := json.Marshal(series[end])
		if err != nil {
			return prepared, start, fmt.Errorf("marshal Datadog series %d: %w", end, err)
		}

		separatorBytes := 0
		if len(offsets) > 0 {
			separatorBytes = 1
		}
		projected := raw.Len() + separatorBytes + len(encoded) + len(datadogPayloadSuffix)
		if len(offsets) > 0 && projected > rawTarget {
			break
		}
		if projected > rawHardLimit {
			return prepared, start, fmt.Errorf("Datadog series %d requires a %d-byte payload, exceeding the %d-byte limit", end, projected, rawHardLimit)
		}

		if separatorBytes != 0 {
			raw.WriteByte(',')
		}
		raw.Write(encoded)
		offsets = append(offsets, raw.Len())
		end++
	}

	if len(offsets) == 0 {
		return prepared, start, fmt.Errorf("could not fit Datadog series %d in a payload", start)
	}
	raw.Write(datadogPayloadSuffix)

	if !compress {
		prepared = preparedDatadogPayload{
			body:              raw.Bytes(),
			seriesCount:       len(offsets),
			uncompressedBytes: raw.Len(),
			compressedBytes:   raw.Len(),
		}
		return prepared, end, nil
	}

	rawBytes := raw.Bytes()
	compressed, err := gzipDatadogPayload(rawBytes)
	if err != nil {
		return prepared, start, err
	}

	count := len(offsets)
	for len(compressed) > limits.targetCompressed && count > 1 {
		keep := int(float64(count) * float64(limits.targetCompressed) / float64(len(compressed)) * 0.90)
		if keep >= count {
			keep = count - 1
		}
		if keep < 1 {
			keep = 1
		}

		count = keep
		end = start + count
		rawBytes = payloadPrefixForSeries(rawBytes, offsets[count-1])
		compressed, err = gzipDatadogPayload(rawBytes)
		if err != nil {
			return prepared, start, err
		}
	}

	if len(rawBytes) > limits.maxDecompressed {
		return prepared, start, fmt.Errorf("Datadog payload is %d bytes decompressed, exceeding the %d-byte limit", len(rawBytes), limits.maxDecompressed)
	}
	if len(compressed) > limits.maxCompressed {
		return prepared, start, fmt.Errorf("Datadog payload is %d bytes compressed, exceeding the %d-byte limit", len(compressed), limits.maxCompressed)
	}

	prepared = preparedDatadogPayload{
		body:              compressed,
		seriesCount:       count,
		uncompressedBytes: len(rawBytes),
		compressedBytes:   len(compressed),
		compressed:        true,
	}
	return prepared, end, nil
}

func payloadPrefixForSeries(raw []byte, end int) []byte {
	prefix := make([]byte, end+len(datadogPayloadSuffix))
	copy(prefix, raw[:end])
	copy(prefix[end:], datadogPayloadSuffix)
	return prefix
}

func gzipDatadogPayload(raw []byte) ([]byte, error) {
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	if _, err := w.Write(raw); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

func (s *Datadog) sendAPI(ctx context.Context, series []datadogV2.MetricSeries, metrics *blip.Metrics) error {
	if s.submitter == nil {
		return fmt.Errorf("Datadog API submitter is not configured")
	}

	batchStart := time.Now()
	rangeStart := 0
	chunk := 0
	for rangeStart < len(series) {
		chunk++
		maxSeries := int(s.maxSeriesPerRequest.Load())
		if maxSeries <= 0 {
			maxSeries = s.payloadLimits.maxSeries
		}

		prepareStart := time.Now()
		payload, rangeEnd, err := prepareDatadogPayload(ctx, series, rangeStart, maxSeries, s.compress, s.payloadLimits)
		if err != nil {
			return err
		}
		prepareDuration := time.Since(prepareStart)

		attempt := 0
		for {
			s.event.Sendf(event.SINK_PAYLOAD,
				"plan=%s level=%s interval=%d chunk=%d attempt=%d series=%d raw-bytes=%d wire-bytes=%d prepare=%s",
				metrics.Plan, metrics.Level, metrics.Interval, chunk, attempt+1, payload.seriesCount,
				payload.uncompressedBytes, payload.compressedBytes, prepareDuration)
			status.Monitor(s.monitorId, s.Name(),
				"%s/%s/%d: chunk %d sending %d series (%d raw bytes, %d wire bytes)",
				metrics.Plan, metrics.Level, metrics.Interval, chunk, payload.seriesCount,
				payload.uncompressedBytes, payload.compressedBytes)

			sendStart := time.Now()
			result, err := s.submitter.Submit(ctx, payload)
			sendDuration := time.Since(sendStart)
			if err == nil {
				s.event.Sendf(event.SINK_PAYLOAD,
					"plan=%s level=%s interval=%d chunk=%d attempt=%d status=%d series=%d raw-bytes=%d wire-bytes=%d send=%s",
					metrics.Plan, metrics.Level, metrics.Interval, chunk, attempt+1, result.statusCode,
					payload.seriesCount, payload.uncompressedBytes, payload.compressedBytes, sendDuration)
				if len(result.errors) > 0 {
					s.event.Errorf(event.SINK_SERVER_ERROR, "Datadog returned success and %d errors: %s", len(result.errors), strings.Join(result.errors, ", "))
				}
				rangeStart = rangeEnd
				break
			}

			if result.statusCode != http.StatusRequestEntityTooLarge {
				if result.statusCode == 0 {
					return fmt.Errorf("network error (nil response): %w", err)
				}
				return err
			}

			attempt++
			s.event.Sendf(event.SINK_PAYLOAD,
				"plan=%s level=%s interval=%d chunk=%d attempt=%d status=413 series=%d raw-bytes=%d wire-bytes=%d send=%s",
				metrics.Plan, metrics.Level, metrics.Interval, chunk, attempt,
				payload.seriesCount, payload.uncompressedBytes, payload.compressedBytes, sendDuration)
			if payload.seriesCount == 1 || attempt > datadogMax413Retries {
				return fmt.Errorf("Datadog rejected %d locally-sized series with HTTP 413 after %d attempts: raw-bytes=%d wire-bytes=%d: %w",
					payload.seriesCount, attempt, payload.uncompressedBytes, payload.compressedBytes, err)
			}

			maxSeries = payload.seriesCount / 2
			if maxSeries < 1 {
				maxSeries = 1
			}
			s.reduceMaxSeriesPerRequest(maxSeries)

			prepareStart = time.Now()
			payload, rangeEnd, err = prepareDatadogPayload(ctx, series, rangeStart, maxSeries, s.compress, s.payloadLimits)
			if err != nil {
				return err
			}
			prepareDuration = time.Since(prepareStart)
		}
	}

	s.event.Sendf(event.SINK_PAYLOAD,
		"plan=%s level=%s interval=%d chunks=%d series=%d total=%s",
		metrics.Plan, metrics.Level, metrics.Interval, chunk, len(series), time.Since(batchStart))
	return nil
}

func (s *Datadog) reduceMaxSeriesPerRequest(limit int) {
	for {
		current := s.maxSeriesPerRequest.Load()
		if current > 0 && int64(limit) >= current {
			return
		}
		if s.maxSeriesPerRequest.CompareAndSwap(current, int64(limit)) {
			return
		}
	}
}
