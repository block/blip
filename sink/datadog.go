// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/DataDog/datadog-go/v5/statsd"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/sink/tr"
	"github.com/cashapp/blip/v2/status"
)

var portRe = regexp.MustCompile(`:\d+$`)

const (
	MAX_PAYLOAD_SIZE int = datadogMaxCompressedPayloadSize
)

// Datadog sends metrics to Datadog.
type Datadog struct {
	monitorId string
	tags      []string            // monitor.tags (dimensions)
	tr        tr.DomainTranslator // datadog.metric-translator
	prefix    string              // datadog.metric-prefix
	event     event.MonitorReceiver

	// -- Api
	apiKeyAuth          string
	appKeyAuth          string
	resources           []datadogV2.MetricResource
	compress            bool
	submitter           datadogMetricSubmitter
	payloadLimits       datadogPayloadLimits
	maxSeriesPerRequest atomic.Int64

	// -- DogStatsD
	dogstatsd       bool
	dogstatsdClient *statsd.Client
	dogstatsdHost   string
}

func NewDatadog(monitorId string, opts, tags map[string]string, httpClient *http.Client) (*Datadog, error) {
	tagList := make([]string, 0, len(tags))
	var resources []datadogV2.MetricResource = nil

	for k, v := range tags {
		tagList = append(tagList, fmt.Sprintf("%s:%s", k, v))

		// If we have a "host" tag, we should include a resource definition for host
		// so that metrics are properly associated with infrastructure in Datadog
		if k == "host" {
			resources = []datadogV2.MetricResource{
				{
					Name: datadog.PtrString(v),
					Type: datadog.PtrString("host"),
				},
			}
		}
	}
	sort.Strings(tagList)

	d := &Datadog{
		monitorId:     monitorId,
		event:         event.MonitorReceiver{MonitorId: monitorId},
		tags:          tagList,
		resources:     resources,
		compress:      true,
		payloadLimits: defaultDatadogPayloadLimits(),
	}
	d.maxSeriesPerRequest.Store(int64(d.payloadLimits.maxSeries))

	for k, v := range opts {
		switch k {
		case "api-key-auth":
			d.apiKeyAuth = v

		case "api-key-auth-file":
			bytes, err := os.ReadFile(v)
			if err != nil {
				return nil, err
			} else {
				d.apiKeyAuth = string(bytes)
			}

		case "app-key-auth":
			d.appKeyAuth = v

		case "app-key-auth-file":
			bytes, err := os.ReadFile(v)
			if err != nil {
				return nil, err
			} else {
				d.appKeyAuth = string(bytes)
			}

		case "metric-translator":
			tr, err := tr.Make(v)
			if err != nil {
				return nil, err
			}
			d.tr = tr

		case "metric-prefix":
			if v == "" {
				return nil, fmt.Errorf("datadog sink metric-prefix is empty string; value required when option is specified")
			}
			d.prefix = v

		case "api-compress":
			d.compress = blip.Bool(v)
		case "dogstatsd-host":
			d.dogstatsdHost = v

		default:
			return nil, fmt.Errorf("invalid option: %s", k)
		}
	}

	if d.dogstatsdHost != "" {
		d.dogstatsd = true
	}

	// if DogStatsD and api are both setup, return error as it will otherwise result in duplicate metrics
	if d.dogstatsd && (d.apiKeyAuth != "" && d.appKeyAuth != "") {
		return nil, fmt.Errorf("datadog sink requires either dogstatsd host or (api-key-auth and app-key-auth), not both at the same time")
	}

	if d.dogstatsd {
		if !portRe.MatchString(d.dogstatsdHost) {
			d.dogstatsdHost += ":8125"
		}
		client, err := statsd.New(d.dogstatsdHost)
		if err != nil {
			return nil, err
		}
		d.dogstatsdClient = client
	} else {
		if d.apiKeyAuth == "" {
			return nil, fmt.Errorf("datadog sink requires either api-key-auth or api-key-auth-file")
		}

		if d.appKeyAuth == "" {
			return nil, fmt.Errorf("datadog sink requires either app-key-auth or app-key-auth-file")
		}

		c := datadog.NewConfiguration()
		c.HTTPClient = httpClient
		c.Compress = d.compress
		d.submitter = &datadogAPISubmitter{
			client: datadog.NewAPIClient(c),
			apiKey: d.apiKeyAuth,
		}
	}

	return d, nil
}

func (s *Datadog) Send(ctx context.Context, m *blip.Metrics) error {
	return s.send(ctx, m, true)
}

func (s *Datadog) send(ctx context.Context, m *blip.Metrics, allowCheckpoint bool) error {
	status.Monitor(s.monitorId, s.Name(), "sending metrics")

	// Pre-alloc data points if using Datadog API (not DogStatsD)
	var dp []datadogV2.MetricSeries
	var n int
	for _, metrics := range m.Values {
		n += len(metrics)
	}
	// Return nil is no metric values. This happens when collection runs but
	// MySQL is offline (or some other error), so metrics were collected.
	if n == 0 {
		blip.Debug("%s: zero metric values collect: %s", m)
		return nil
	}
	if !s.dogstatsd && allowCheckpoint {
		_, err := s.SendWithCheckpoint(ctx, m, nil)
		return err
	}
	if !s.dogstatsd {
		dp = make([]datadogV2.MetricSeries, n)
	}
	n = 0

	// On return, set monitor status for this sink
	defer func() {
		status.Monitor(s.monitorId, s.Name(), "last sent %d metrics at %s", n, time.Now())
	}()

	// Convert Blip metric values to Datadog data points
	for domain := range m.Values { // each domain
		metrics := m.Values[domain]
		var name string

	METRICS:
		for i := range metrics { // each metric in this domain

			// Set full metric name: translator (if any) else Blip standard,
			// then prefix (if any)
			if s.tr == nil {
				name = domain + "." + metrics[i].Name
			} else {
				name = s.tr.Translate(domain, metrics[i].Name)
			}
			if s.prefix != "" {
				name = s.prefix + name
			}

			// Copy metric meta and groups into tags (dimensions), if any
			var tags []string
			if len(metrics[i].Meta) == 0 && len(metrics[i].Group) == 0 {
				// Optimization: if no meta or group, then reuse pointer to
				// s.tags which points to the tags--never modify s.tags!
				tags = s.tags
			} else {
				// There are meta or groups (or both), so we MUST COPY tags
				// from s.tags and the rest into a new map
				tags = make([]string, 0, len(s.tags)+len(metrics[i].Meta)+len(metrics[i].Group))
				for _, v := range s.tags { // copy tags (from config)
					tags = append(tags, v)
				}

				for k, v := range metrics[i].Meta { // metric meta
					if k == "ts" { // avoid time series explosion: ts is high cardinality
						continue
					}
					tags = append(tags, fmt.Sprintf("%s:%s", k, v))
				}

				for k, v := range metrics[i].Group { // metric groups
					tags = append(tags, fmt.Sprintf("%s:%s", k, v))
				}
			}

			var timestamp int64

			// Datadog requires a timestamp when creating a data point
			if tsStr, ok := metrics[i].Meta["ts"]; !ok {
				timestamp = m.Begin.Unix()
			} else {
				var err error
				msTs, err := strconv.ParseInt(tsStr, 10, 64) // ts in milliseconds, string -> int64
				if err != nil {
					blip.Debug("invalid timestamp for %s %s: %s: %s", domain, metrics[i].Name, tsStr, err)
					continue METRICS
				}
				timestamp = msTs / 1000 // convert to seconds
			}

			// Convert Blip metric type to Datadog metric type
			switch metrics[i].Type {
			case blip.CUMULATIVE_COUNTER, blip.DELTA_COUNTER:
				// This sinks is wrapped in a Delta pseudo-sink, so
				// do NOT calculate delta values here; it's already
				// done on a per-domain basis.
				if s.dogstatsd {
					err := s.dogstatsdClient.Count(name, int64(metrics[i].Value), tags, 1)
					if err != nil {
						blip.Debug("error sending data points to Datadog: %s", err)
					}
				} else {
					dp[n] = datadogV2.MetricSeries{
						Metric: name,
						Type:   datadogV2.METRICINTAKETYPE_COUNT.Ptr(),
						Points: []datadogV2.MetricPoint{
							{
								Value:     datadog.PtrFloat64(metrics[i].Value),
								Timestamp: datadog.PtrInt64(timestamp),
							},
						},
						Tags:      tags,
						Resources: s.resources,
					}
				}

			case blip.GAUGE, blip.BOOL:
				if s.dogstatsd {
					err := s.dogstatsdClient.Gauge(name, metrics[i].Value, tags, 1)
					if err != nil {
						blip.Debug("error sending data points to Datadog: %s", err)
					}
				} else {
					dp[n] = datadogV2.MetricSeries{
						Metric: name,
						Type:   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
						Points: []datadogV2.MetricPoint{
							{
								Value:     datadog.PtrFloat64(metrics[i].Value),
								Timestamp: datadog.PtrInt64(timestamp),
							},
						},
						Tags:      tags,
						Resources: s.resources,
					}
				}

			default:
				// datadog doesn't support this Blip metric type, so skip it
				continue METRICS // @todo error?
			}

			n++
		} // metric
	} // domain

	// This shouldn't happen: >0 Blip metrics in but =0 Datadog data points out
	if n == 0 {
		errMsg := fmt.Sprintf("zero data points created after processing Blip metrics: %s", m)
		s.event.Errorf(event.SINK_INVALID_METRICS, "%s", errMsg)
		return nil // do not retry
	}

	// dogstatsd metrics are sent to the Datadog agent inside the loop, there's nothing else to do
	if s.dogstatsd {
		return nil // success (dogstatsd)
	}

	return s.sendAPI(ctx, dp[:n], m)
}

func (s *Datadog) sendDogStatsD(ctx context.Context, m *blip.Metrics) error {
	return s.send(ctx, m, false)
}

func (s *Datadog) Name() string {
	return "datadog"
}
