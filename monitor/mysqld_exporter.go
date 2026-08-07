// Copyright 2024 Block, Inc.

package monitor

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/prom"
)

// Exporter emulates a Prometheus mysqld_exporter. It implements prom.Exporter.
type Exporter struct {
	cfg    blip.ConfigExporter
	plan   blip.Plan
	engine *Engine
	// --
	promRegistry *prometheus.Registry
	*sync.Mutex
	prepared bool
	event    event.MonitorReceiver
	interval uint
}

var _ prom.Exporter = Exporter{}

func NewExporter(cfg blip.ConfigExporter, plan blip.Plan, engine *Engine) *Exporter {
	e := &Exporter{
		cfg:          cfg,
		plan:         plan,
		engine:       engine,
		promRegistry: prometheus.NewRegistry(),
		Mutex:        &sync.Mutex{},
		event:        event.MonitorReceiver{MonitorId: engine.MonitorId()},
	}
	e.promRegistry.MustRegister(e)
	return e
}

func (e Exporter) Plan() blip.Plan {
	return e.plan
}

// --------------------------------------------------------------------------
// Implement Prometheus collector

// Scrape collects and returns metrics in Prometheus exposition format.
// This function is called in response to GET /metrics.
func (e Exporter) Scrape() (string, error) {
	// Gather calls the Collect method of the exporter
	mfs, err := e.promRegistry.Gather()
	if err != nil {
		return "", fmt.Errorf("Unable to convert blip metrics to Prom metrics. Error: %s", err)
	}

	// Converts the MetricFamily protobufs to prom text format.
	var buf bytes.Buffer
	for _, mf := range mfs {
		expfmt.MetricFamilyToText(&buf, mf)
	}

	return buf.String(), nil
}

func (e Exporter) Describe(descs chan<- *prometheus.Desc) {
	// Left empty intentionally to make the collector unchecked.
}

var noop = func() {}

// Collect collects metrics. It is called indirectly via Scrape.
func (e Exporter) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // @todo make configurable
	defer cancel()

	e.Lock()
	if !e.prepared {
		if err := e.engine.Prepare(ctx, e.plan, noop, noop); err != nil {
			blip.Debug(err.Error())
			e.Unlock()
			return
		}
		e.prepared = true
	}
	e.Unlock()

	e.interval += 1
	metrics, err := e.engine.Collect(ctx, e.interval, "prom", time.Now())
	if err != nil {
		e.event.Errorf(event.ENGINE_COLLECT_ERROR, "%s; see monitor status or event log for details", err)
	}

	for domain, vals := range metrics[0].Values {
		tr := prom.Translator(domain)
		if tr == nil {
			blip.Debug("no translator registered for %s", domain)
			continue
		}
		tr.Translate(vals, ch)
	}
}

// Stop releases collector resources prepared by the exporter engine. The
// monitor calls this only after the HTTP API has stopped accepting and serving
// scrapes.
func (e *Exporter) Stop() {
	e.Lock()
	defer e.Unlock()
	e.engine.Stop()
	e.prepared = false
}
