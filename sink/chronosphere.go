// Copyright 2024 Block, Inc.

package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	om "github.com/cashapp/blip/v2/openmetrics"
	"github.com/cashapp/blip/v2/prom"
	"github.com/cashapp/blip/v2/status"
)

const DEFAULT_CHRONOSPHERE_URL = "http://127.0.0.1:3030/openmetrics/write"

// Chronosphere sends metrics to Chronosphere (https://chronosphere.io) using OpenMetrics.
type Chronosphere struct {
	monitorId string
	tags      map[string]string
	// --
	url      string
	labels   []*om.Label
	debug    bool
	strictTr bool
	event    event.MonitorReceiver
}

func NewChronosphere(monitorId string, opts, tags map[string]string) (*Chronosphere, error) {
	s := &Chronosphere{
		monitorId: monitorId,
		tags:      tags,
		// --
		url:   DEFAULT_CHRONOSPHERE_URL,
		event: event.MonitorReceiver{MonitorId: monitorId},
	}

	for k, v := range opts {
		switch k {
		case "url":
			s.url = v
		case "strict-tr":
			s.strictTr = blip.Bool(v)
		case "debug":
			s.debug = blip.Bool(v)
		default:
			return nil, fmt.Errorf("invalid option: %s", k)
		}
	}

	if len(tags) > 0 {
		s.labels = make([]*om.Label, len(tags))
		i := 0
		for k, v := range tags {
			s.labels[i] = &om.Label{
				Name:  k,
				Value: v,
			}
			i++
		}
	}

	return s, nil
}

var nameRe = regexp.MustCompile("([^a-zA-Z0-9_])")

// omName converts a Blip domain and metric name to OpenMetrics convention.
func omName(s string) string {
	return strings.ToLower(strings.ToLower(nameRe.ReplaceAllString(s, "_")))
}

func (s *Chronosphere) Send(ctx context.Context, m *blip.Metrics) (lerr error) {
	status.Monitor(s.monitorId, "chronosphere", "sending metrics from %s", m.Begin)

	n := 0
	defer func() {
		if lerr == nil {
			status.Monitor(s.monitorId, "chronosphere", "last sent %d metrics at %s", n, time.Now())
		} else {
			s.event.Errorf(event.SINK_SEND_ERROR, "%s", lerr.Error())
			status.Monitor(s.monitorId, "chronosphere", "error on last send at %s: %s", time.Now(), lerr)
		}
	}()

	ts := timestamppb.New(m.Begin) // Go timestamp to protobuf timestamp
	// @todo check blip.Metrics.Meta[ts] and use if set

	// Counter number of Blip metric values so we can pre-alloc OpenMetrics
	// structs--just an easy micro-optimization to avoid unnecessary memory
	// alloc using Go append(), because OpenMetrics structs are big
	for _, metrics := range m.Values {
		n += len(metrics)
	}

	// ----------------------------------------------------------------------
	// Build OpenMetrics data
	// ----------------------------------------------------------------------

	// Top-level struct is MetricSet that contains a MetricFamily for each metric,
	// i.e. MetricFamily is one metric (like Threads_running). So we need one
	// MetricFamily struct for each metric, as counted above.
	fam := make([]*om.MetricFamily, n)
	set := &om.MetricSet{
		MetricFamilies: fam,
	}

	// Create the MetricFamily for each Blip metric. Blip metrics are grouped by
	// domain (e.g. var.global), and each domain has several metrics. This two-level
	// hierarchy is flattened to a single list of unique metrics by combining
	// domain name and metric name, modified to fit OpenMetrics requirements.
	n = 0 // index into fam[n]
	for domain, metricValues := range m.Values {

		// Prometheus translator (tr) for this Blip domain. The tr determines
		// how Blip naming changes to match Prometheus/OpenMetric naming
		// convention, which is metric_names_like_this. We use a prefix, "mysql",
		// and a shorter, simpler domain name. E.g. status.global.threads_running
		// becomes mysql_status_threads_running.
		tr := prom.Translator(domain)
		if tr == nil {
			err := fmt.Errorf("no translator for %s", domain)
			if s.strictTr {
				lerr = err
				return // implicit lerr
			}
			blip.Debug(err.Error() + ", ignoring (strict-tr = false)")
			continue
		}
		prefix, _, shortDomain := tr.Names()

		// For each Blip metric (in this domain), make an OpenMetric MetricFamily
		// struct, which really is as deeply nested as this:
	METRICS:
		for _, m := range metricValues {

			// One metric with one value:
			fam[n] = &om.MetricFamily{
				Name: omName(prefix + "_" + shortDomain + "_" + m.Name), // METRIC NAME
				Metrics: []*om.Metric{
					{
						Labels: s.labels, // pre-created in NewChronosphere
						MetricPoints: []*om.MetricPoint{
							{
								Timestamp: ts,
								Value:     nil, // VALUE assigned below
							},
						},
					},
				},
			}

			// Assign value based on type because the structs are different
			switch m.Type {
			case blip.GAUGE, blip.BOOL:
				fam[n].Metrics[0].MetricPoints[0].Value = &om.MetricPoint_GaugeValue{
					GaugeValue: &om.GaugeValue{
						Value: &om.GaugeValue_DoubleValue{
							DoubleValue: m.Value, // VALUE (gauge)
						},
					},
				}
			case blip.CUMULATIVE_COUNTER:
				fam[n].Metrics[0].MetricPoints[0].Value = &om.MetricPoint_CounterValue{
					CounterValue: &om.CounterValue{
						Total: &om.CounterValue_DoubleValue{
							DoubleValue: m.Value, // VALUE (counter)
						},
					},
				}
			default:
				// Chronosphere (or OpenMetrics) doesn't support this (includes DELTA_COUNTER) Blip metric type, so skip it
				// TODO: Either error out or maintain cumulative value from delta
				continue METRICS
			}

			n++ // next metric: fam[n]
		} // each metric in a Blip domain
	} // each Blip domain

	// If config.sinks.chronosphere.debug=true, then just print via debug, don't send
	if s.debug {
		blip.Debug(set.String())
		return // success
	}

	// ----------------------------------------------------------------------
	// Send OpenMetrics data to Chronosphere
	// ----------------------------------------------------------------------

	// First, marshal the OpenMetrics data
	data, err := proto.Marshal(set)
	if err != nil {
		lerr = err
		return // implicit lerr
	}

	// Second, compress data with Snappy
	buf := bytes.NewBuffer(snappy.Encode(nil, data))

	// Last, HTTP POST the compressed data to Chronosphere collector
	resp, err := http.Post(s.url, "application/octet-stream", buf)
	if err != nil {
		lerr = err
		return // implicit lerr
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		lerr = fmt.Errorf("error reading response to POST: %s", err)
		return // implicit lerr
	}
	if resp.StatusCode >= 300 {
		lerr = fmt.Errorf("chronocollector HTTP response code %d, expected 2xx: %s",
			resp.StatusCode, string(body))
		return // implicit lerr
	}

	return // success
}

func (s *Chronosphere) Name() string {
	return "chronosphere"
}
