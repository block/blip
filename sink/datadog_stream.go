// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/status"
)

// datadogMetricCursor identifies the next Blip metric to convert. It is kept
// in the retry queue as part of an opaque checkpoint, so a retry starts after
// the last chunk acknowledged by Datadog.
type datadogMetricCursor struct {
	domain int
	metric int
}

type datadogSendCheckpoint struct {
	domains    []string
	cursor     datadogMetricCursor
	chunks     int
	sentSeries int
	maxPending int
	started    time.Time

	// pending is bounded by payloadLimits.maxSeries. Keeping an already
	// converted window avoids translating the unused suffix again when the byte
	// budget selects only a prefix, and makes retries reproduce that suffix.
	pending        []datadogV2.MetricSeries
	pendingCursors []datadogMetricCursor
	pendingFinal   datadogMetricCursor
	pendingDone    bool
}

// SendWithCheckpoint converts only a bounded window of Blip metrics at a time.
// The returned checkpoint advances only after Datadog acknowledges a chunk.
func (s *Datadog) SendWithCheckpoint(ctx context.Context, metrics *blip.Metrics, checkpoint any) (any, error) {
	if s.dogstatsd {
		return nil, s.sendDogStatsD(ctx, metrics)
	}
	if s.submitter == nil {
		return checkpoint, fmt.Errorf("Datadog API submitter is not configured")
	}
	if err := s.payloadLimits.validate(); err != nil {
		return checkpoint, err
	}
	totalValues := 0
	for _, values := range metrics.Values {
		totalValues += len(values)
	}
	if totalValues == 0 {
		blip.Debug("%s: zero metric values collect: %s", metrics)
		return nil, nil
	}

	state, err := newDatadogSendCheckpoint(metrics, checkpoint)
	if err != nil {
		return checkpoint, err
	}
	status.Monitor(s.monitorId, s.Name(), "sending metrics")

	for {
		maxSeries := int(s.maxSeriesPerRequest.Load())
		if maxSeries <= 0 || maxSeries > s.payloadLimits.maxSeries {
			maxSeries = s.payloadLimits.maxSeries
		}

		if len(state.pending) < maxSeries && !state.pendingDone {
			collectFrom := state.cursor
			if len(state.pendingCursors) > 0 {
				collectFrom = state.pendingCursors[len(state.pendingCursors)-1]
			}
			var converted []datadogV2.MetricSeries
			var cursors []datadogMetricCursor
			converted, cursors, state.pendingFinal, state.pendingDone, err = s.collectDatadogSeries(ctx, metrics, state.domains, collectFrom, maxSeries-len(state.pending))
			if err != nil {
				return state, err
			}
			if len(state.pending) == 0 {
				state.pending = converted
				state.pendingCursors = cursors
			} else {
				state.pending = append(state.pending, converted...)
				state.pendingCursors = append(state.pendingCursors, cursors...)
			}
			if len(state.pending) > state.maxPending {
				state.maxPending = len(state.pending)
			}
		}
		if len(state.pending) == 0 {
			if state.sentSeries == 0 {
				errMsg := fmt.Sprintf("zero data points created after processing Blip metrics: %s", metrics)
				s.event.Errorf(event.SINK_INVALID_METRICS, "%s", errMsg)
			}
			status.Monitor(s.monitorId, s.Name(), "last sent %d metrics at %s", state.sentSeries, time.Now())
			s.event.Sendf(event.SINK_PAYLOAD,
				"plan=%s level=%s interval=%d chunks=%d series=%d max-window-series=%d total=%s conversion=streaming",
				metrics.Plan, metrics.Level, metrics.Interval, state.chunks, state.sentSeries, state.maxPending, time.Since(state.started))
			return nil, nil
		}

		chunk := state.chunks + 1
		prepareStart := time.Now()
		payload, rangeEnd, err := prepareDatadogPayload(ctx, state.pending, 0, maxSeries, s.compress, s.payloadLimits)
		if err != nil {
			return state, err
		}
		prepareDuration := time.Since(prepareStart)

		attempt := 0
		for {
			s.event.Sendf(event.SINK_PAYLOAD,
				"plan=%s level=%s interval=%d chunk=%d attempt=%d series=%d raw-bytes=%d wire-bytes=%d prepare=%s conversion=streaming",
				metrics.Plan, metrics.Level, metrics.Interval, chunk, attempt+1, payload.seriesCount,
				payload.uncompressedBytes, payload.compressedBytes, prepareDuration)
			status.Monitor(s.monitorId, s.Name(),
				"%s/%s/%d: chunk %d sending %d series (%d raw bytes, %d wire bytes)",
				metrics.Plan, metrics.Level, metrics.Interval, chunk, payload.seriesCount,
				payload.uncompressedBytes, payload.compressedBytes)

			sendStart := time.Now()
			result, submitErr := s.submitter.Submit(ctx, payload)
			sendDuration := time.Since(sendStart)
			if submitErr == nil {
				s.event.Sendf(event.SINK_PAYLOAD,
					"plan=%s level=%s interval=%d chunk=%d attempt=%d status=%d series=%d raw-bytes=%d wire-bytes=%d send=%s conversion=streaming",
					metrics.Plan, metrics.Level, metrics.Interval, chunk, attempt+1, result.statusCode,
					payload.seriesCount, payload.uncompressedBytes, payload.compressedBytes, sendDuration)
				if len(result.errors) > 0 {
					s.event.Errorf(event.SINK_SERVER_ERROR, "Datadog returned success and %d errors: %s", len(result.errors), strings.Join(result.errors, ", "))
				}

				state.cursor = state.pendingCursors[rangeEnd-1]
				if state.pendingDone && rangeEnd == len(state.pending) {
					state.cursor = state.pendingFinal
				}
				remaining := copy(state.pending, state.pending[rangeEnd:])
				for i := remaining; i < len(state.pending); i++ {
					state.pending[i] = datadogV2.MetricSeries{}
				}
				state.pending = state.pending[:remaining]
				remaining = copy(state.pendingCursors, state.pendingCursors[rangeEnd:])
				state.pendingCursors = state.pendingCursors[:remaining]
				state.chunks++
				state.sentSeries += payload.seriesCount
				break
			}

			if result.statusCode != http.StatusRequestEntityTooLarge {
				if result.statusCode == 0 {
					return state, fmt.Errorf("network error (nil response): %w", submitErr)
				}
				return state, submitErr
			}

			attempt++
			s.event.Sendf(event.SINK_PAYLOAD,
				"plan=%s level=%s interval=%d chunk=%d attempt=%d status=413 series=%d raw-bytes=%d wire-bytes=%d send=%s conversion=streaming",
				metrics.Plan, metrics.Level, metrics.Interval, chunk, attempt,
				payload.seriesCount, payload.uncompressedBytes, payload.compressedBytes, sendDuration)
			if payload.seriesCount == 1 || attempt > datadogMax413Retries {
				return state, fmt.Errorf("Datadog rejected %d locally-sized series with HTTP 413 after %d attempts: raw-bytes=%d wire-bytes=%d: %w",
					payload.seriesCount, attempt, payload.uncompressedBytes, payload.compressedBytes, submitErr)
			}

			maxSeries = payload.seriesCount / 2
			if maxSeries < 1 {
				maxSeries = 1
			}
			s.reduceMaxSeriesPerRequest(maxSeries)

			prepareStart = time.Now()
			payload, rangeEnd, err = prepareDatadogPayload(ctx, state.pending, 0, maxSeries, s.compress, s.payloadLimits)
			if err != nil {
				return state, err
			}
			prepareDuration = time.Since(prepareStart)
		}

		if state.pendingDone && len(state.pending) == 0 && state.cursor == state.pendingFinal {
			status.Monitor(s.monitorId, s.Name(), "last sent %d metrics at %s", state.sentSeries, time.Now())
			s.event.Sendf(event.SINK_PAYLOAD,
				"plan=%s level=%s interval=%d chunks=%d series=%d max-window-series=%d total=%s conversion=streaming",
				metrics.Plan, metrics.Level, metrics.Interval, state.chunks, state.sentSeries, state.maxPending, time.Since(state.started))
			return nil, nil
		}
	}
}

func newDatadogSendCheckpoint(metrics *blip.Metrics, checkpoint any) (*datadogSendCheckpoint, error) {
	if checkpoint != nil {
		state, ok := checkpoint.(*datadogSendCheckpoint)
		if !ok {
			return nil, fmt.Errorf("invalid Datadog send checkpoint type %T", checkpoint)
		}
		return state, nil
	}

	domains := make([]string, 0, len(metrics.Values))
	for domain := range metrics.Values {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	return &datadogSendCheckpoint{domains: domains, started: time.Now()}, nil
}

func (s *Datadog) collectDatadogSeries(ctx context.Context, metrics *blip.Metrics, domains []string, start datadogMetricCursor, limit int) ([]datadogV2.MetricSeries, []datadogMetricCursor, datadogMetricCursor, bool, error) {
	series := make([]datadogV2.MetricSeries, 0, limit)
	cursors := make([]datadogMetricCursor, 0, limit)
	cursor := start

	for cursor.domain < len(domains) {
		values := metrics.Values[domains[cursor.domain]]
		for cursor.metric < len(values) {
			select {
			case <-ctx.Done():
				return nil, nil, start, false, ctx.Err()
			default:
			}

			value := values[cursor.metric]
			cursor.metric++
			converted, ok := s.datadogMetricSeries(metrics, domains[cursor.domain], value)
			if !ok {
				continue
			}
			series = append(series, converted)
			cursors = append(cursors, cursor)
			if len(series) == limit {
				return series, cursors, cursor, false, nil
			}
		}
		cursor.domain++
		cursor.metric = 0
	}

	return series, cursors, cursor, true, nil
}

func (s *Datadog) datadogMetricSeries(metrics *blip.Metrics, domain string, value blip.MetricValue) (datadogV2.MetricSeries, bool) {
	name := domain + "." + value.Name
	if s.tr != nil {
		name = s.tr.Translate(domain, value.Name)
	}
	if s.prefix != "" {
		name = s.prefix + name
	}

	timestamp := metrics.Begin.Unix()
	if tsStr, ok := value.Meta["ts"]; ok {
		msTs, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			blip.Debug("invalid timestamp for %s %s: %s: %s", domain, value.Name, tsStr, err)
			return datadogV2.MetricSeries{}, false
		}
		timestamp = msTs / 1000
	}

	var metricType *datadogV2.MetricIntakeType
	switch value.Type {
	case blip.CUMULATIVE_COUNTER, blip.DELTA_COUNTER:
		metricType = datadogV2.METRICINTAKETYPE_COUNT.Ptr()
	case blip.GAUGE, blip.BOOL:
		metricType = datadogV2.METRICINTAKETYPE_GAUGE.Ptr()
	default:
		return datadogV2.MetricSeries{}, false
	}

	tags := s.tags
	if len(value.Meta) != 0 || len(value.Group) != 0 {
		tags = make([]string, 0, len(s.tags)+len(value.Meta)+len(value.Group))
		tags = append(tags, s.tags...)
		keys := make([]string, 0, len(value.Meta))
		for key := range value.Meta {
			if key != "ts" {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			tags = append(tags, fmt.Sprintf("%s:%s", key, value.Meta[key]))
		}
		keys = keys[:0]
		for key := range value.Group {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			tags = append(tags, fmt.Sprintf("%s:%s", key, value.Group[key]))
		}
	}

	return datadogV2.MetricSeries{
		Metric: name,
		Type:   metricType,
		Points: []datadogV2.MetricPoint{{
			Value:     datadog.PtrFloat64(value.Value),
			Timestamp: datadog.PtrInt64(timestamp),
		}},
		Tags:      tags,
		Resources: s.resources,
	}, true
}
