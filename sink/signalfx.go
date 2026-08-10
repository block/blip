// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/sfxclient"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sink/tr"
	"github.com/cashapp/blip/v2/status"
)

// SignalFx sends metrics to SignalFx.
type SignalFx struct {
	monitorId string
	dim       map[string]string   // monitor.tags (dimensions)
	tr        tr.DomainTranslator // signalfx.metric-translator
	prefix    string              // signalfx.metric-prefix
	// --
	sfxSink *sfxclient.HTTPSink
}

func NewSignalFx(monitorId string, opts, tags map[string]string, httpClient *http.Client) (*SignalFx, error) {
	sfxSink := sfxclient.NewHTTPSink()
	sfxSink.Client = httpClient // made by blip.Factory.HTTPClient

	s := &SignalFx{
		sfxSink:   sfxSink,
		monitorId: monitorId,
		dim:       tags,
	}

	for k, v := range opts {
		switch k {
		case "auth-token-file":
			bytes, err := os.ReadFile(v)
			if err != nil {
				return nil, err
			} else {
				sfxSink.AuthToken = string(bytes)
			}
		case "auth-token":
			sfxSink.AuthToken = v
		case "metric-translator":
			tr, err := tr.Make(v)
			if err != nil {
				return nil, err
			}
			s.tr = tr
		case "metric-prefix":
			if v == "" {
				return nil, fmt.Errorf("signalfx sink metric-prefix is empty string; value required when option is specified")
			}
			s.prefix = v
		default:
			return nil, fmt.Errorf("invalid option: %s", k)
		}
	}

	if sfxSink.AuthToken == "" {
		return nil, fmt.Errorf("signalfx sink requires either auth-token or auth-token-file")
	}

	return s, nil
}

func (s *SignalFx) Send(ctx context.Context, m *blip.Metrics) error {
	status.Monitor(s.monitorId, "signalfx", "sending metrics")

	// On return, set monitor status for this sink
	n := 0
	defer func() {
		status.Monitor(s.monitorId, "signalfx", "last sent %d metrics at %s", n, time.Now())
	}()

	// Pre-alloc SFX data points
	for _, metrics := range m.Values {
		n += len(metrics)
	}
	if n == 0 {
		return fmt.Errorf("no Blip metrics were collected")
	}
	dp := make([]*datapoint.Datapoint, n)
	n = 0

	// Convert each Blip metric value to an SFX data point
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
			var dim map[string]string
			if len(metrics[i].Meta) == 0 && len(metrics[i].Group) == 0 {
				// Optimization: if no meta or group, then reuse pointer to
				// s.dim which points to the tags--never modify s.dim!
				dim = s.dim
			} else {
				// There are meta or groups (or both), so we MUST COPY tags
				// from s.dim and the rest into a new map
				dim = make(map[string]string, len(s.dim)+len(metrics[i].Meta)+len(metrics[i].Group))
				for k, v := range s.dim { // copy tags (from config)
					dim[k] = v
				}
				for k, v := range metrics[i].Meta { // metric meta
					if k == "ts" { // avoid time series explosion: ts is high cardinality
						continue
					}
					dim[k] = v
				}
				for k, v := range metrics[i].Group { // metric groups
					dim[k] = v
				}
			}

			// Convert Blip metric type to SFX metric type
			switch metrics[i].Type {
			case blip.CUMULATIVE_COUNTER:
				dp[n] = sfxclient.CumulativeF(name, dim, metrics[i].Value)
			case blip.DELTA_COUNTER:
				dp[n] = sfxclient.Counter(name, dim, int64(metrics[i].Value))
			case blip.GAUGE, blip.BOOL:
				dp[n] = sfxclient.GaugeF(name, dim, metrics[i].Value)
			default:
				// SFX doesn't support this Blip metric type, so skip it
				continue METRICS // @todo error?
			}

			// Always set data point timestamp, else SFX will set it to the time
			// when SFX receives the data points, which could way off if metrics
			// are delayed.
			// https://dev.splunk.com/observability/docs/datamodel/ingest/#Datapoint-timestamps
			// Also, as 'else' block handles: some collectors (e.g. aws.rds) get
			// metrics from the past, so they have there own per-metric timestamp.
			if tsStr, ok := metrics[i].Meta["ts"]; !ok {
				dp[n].Timestamp = m.Begin
			} else {
				tsMs, err := strconv.ParseInt(tsStr, 10, 64) // ts in milliseconds, string -> int64
				if err != nil {
					blip.Debug("invalid timestamp for %s %s: %s: %s", domain, metrics[i].Name, tsStr, err)
					continue METRICS
				}
				dp[n].Timestamp = time.UnixMilli(tsMs)
			}

			n++
		} // metric
	} // domain

	// This shouldn't happen: >0 Blip metrics in but =0 SFX data points out
	if n == 0 {
		return fmt.Errorf("no SignalFx data points after processing %d Blip metrics", len(m.Values))
	}

	// Send metrics to SFX. The SFX client handles everything; we just pass
	// it data points.
	err := s.sfxSink.AddDatapoints(ctx, dp[0:n])
	if err != nil {
		blip.Debug("error sending data points to SignalFx: %s", err)
		s.sfxSink.Client.CloseIdleConnections()
	}

	return err
}

func (s *SignalFx) Name() string {
	return "signalfx"
}
