// Copyright 2024 Block, Inc.

package tr

import (
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cashapp/blip/v2"
)

const GENERIC_PREFIX = "mysql"

// Copied from /percona/mysqld_exporter/collector/global_status.go
var nameRe = regexp.MustCompile("([^a-zA-Z0-9_])")

func validPrometheusName(s string) string {
	s = nameRe.ReplaceAllString(s, "_")
	s = strings.ToLower(s)
	return s
}

type Generic struct {
	Domain      string
	ShortDomain string
}

func (tr Generic) Names() (string, string, string) {
	return GENERIC_PREFIX, tr.Domain, tr.ShortDomain
}

func (tr Generic) Translate(values []blip.MetricValue, ch chan<- prometheus.Metric) {
	for i := range values {
		var promType prometheus.ValueType
		var help string
		switch values[i].Type {
		case blip.CUMULATIVE_COUNTER:
			promType = prometheus.CounterValue
			help = "Generic counter metric."
		case blip.DELTA_COUNTER:
			// Prometheus doesn't have a Delta counter type, skipping
			// TODO: maybe maintain cumulative counter values from deltas
			continue
		case blip.GAUGE:
			promType = prometheus.GaugeValue
			help = "Generic gauge metric."
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(GENERIC_PREFIX, tr.Domain, validPrometheusName(values[i].Name)),
				help,
				nil, nil,
			),
			promType,
			values[i].Value,
		)
	}
}
