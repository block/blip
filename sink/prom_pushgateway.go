// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/prom"
	"github.com/cashapp/blip/v2/status"
)

// PromPushgateway implmements https://github.com/prometheus/pushgateway.
type PromPushgateway struct {
	monitorId string
	// --
	event   event.MonitorReceiver
	pusher  *push.Pusher
	metrics *blip.Metrics
}

func NewPromPushgateway(monitorId string, opts, tags map[string]string) (*PromPushgateway, error) {
	addr := "http://127.0.0.1:9091"
	for k, v := range opts {
		switch k {
		case "addr":
			addr = v
		default:
			return nil, fmt.Errorf("invalid option: %s", k)
		}
	}
	s := &PromPushgateway{
		monitorId: monitorId,
		event:     event.MonitorReceiver{MonitorId: monitorId},
	}
	r := prometheus.NewRegistry()
	r.MustRegister(s)
	s.pusher = push.New(addr, "blip").Gatherer(r)
	s.pusher.Format(expfmt.FmtText) // this will change/break when the prom dep is updated, new value is expfmt.TypeTextPlain
	return s, nil
}

func (s *PromPushgateway) Name() string {
	return "prom-pushgateway"
}

func (s *PromPushgateway) Send(ctx context.Context, m *blip.Metrics) error {
	status.Monitor(s.monitorId, s.Name(), "sending metrics")
	defer func() {
		status.Monitor(s.monitorId, s.Name(), "last sent metrics at %s", time.Now())
	}()
	s.metrics = m
	return s.pusher.Add()
}

func (s *PromPushgateway) Collect(ch chan<- prometheus.Metric) {
	for domain, vals := range s.metrics.Values {
		tr := prom.Translator(domain)
		if tr == nil {
			blip.Debug("no translator registered for %s", domain)
			continue
		}
		tr.Translate(vals, ch)
	}
}

func (s *PromPushgateway) Describe(descs chan<- *prometheus.Desc) {
	// Left empty intentionally to make the collector unchecked.
}
