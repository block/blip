// Copyright 2024 Block, Inc.

package tr

import (
	"regexp"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/cashapp/blip/v2"
)

type StatusGlobal struct {
	Domain      string
	ShortDomain string
}

func (tr StatusGlobal) Names() (string, string, string) {
	return GENERIC_PREFIX, tr.Domain, tr.ShortDomain
}

// Copied from /percona/mysqld_exporter/collector/global_status.go

// Regexp to match various groups of status vars.
var globalStatusRE = regexp.MustCompile(`^(com|handler|connection_errors|innodb_buffer_pool_pages|innodb_rows|performance_schema)_(.*)$`)

func (tr StatusGlobal) Translate(values []blip.MetricValue, ch chan<- prom.Metric) {
	for i := range values {

		var promType prom.ValueType
		var help string
		switch values[i].Type {
		case blip.CUMULATIVE_COUNTER:
			promType = prom.CounterValue
			help = "Generic counter metric from SHOW GLOBAL STATUS."
		case blip.DELTA_COUNTER:
			// Prometheus doesn't have a Delta counter type, skipping
			// TODO: maybe maintain cumulative counter values from deltas
			continue
		case blip.GAUGE:
			promType = prom.GaugeValue
			help = "Generic gauge metric from SHOW GLOBAL STATUS."
		}

		match := globalStatusRE.FindStringSubmatch(values[i].Name)
		if match == nil {
			ch <- prom.MustNewConstMetric(
				prom.NewDesc(
					prom.BuildFQName(GENERIC_PREFIX, tr.Domain, validPrometheusName(values[i].Name)),
					help,
					nil, nil,
				),
				promType,
				values[i].Value,
			)
			continue
		}

		switch match[1] {
		case "com":
			ch <- prom.MustNewConstMetric(
				prom.NewDesc(
					prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "commands_total"),
					"Total number of executed MySQL commands.",
					[]string{"command"}, nil,
				),
				prom.CounterValue,
				values[i].Value,
				match[2],
			)
		case "handler":
			ch <- prom.MustNewConstMetric(
				prom.NewDesc(
					prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "handlers_total"),
					"Total number of executed MySQL handlers.",
					[]string{"handler"}, nil,
				),
				prom.CounterValue,
				values[i].Value,
				match[2],
			)
		case "connection_errors":
			ch <- prom.MustNewConstMetric(
				prom.NewDesc(
					prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "connection_errors_total"),
					"Total number of MySQL connection errors.",
					[]string{"error"}, nil,
				),
				prom.CounterValue,
				values[i].Value,
				match[2],
			)
		case "innodb_buffer_pool_pages":
			switch match[2] {
			case "data", "free", "misc", "old":
				ch <- prom.MustNewConstMetric(
					prom.NewDesc(
						prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "buffer_pool_pages"),
						"Innodb buffer pool pages by state.",
						[]string{"state"}, nil,
					),
					prom.GaugeValue,
					values[i].Value,
					match[2],
				)
			case "dirty":
				ch <- prom.MustNewConstMetric(
					prom.NewDesc(
						prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "buffer_pool_dirty_pages"),
						"Innodb buffer pool dirty pages.",
						[]string{}, nil,
					),
					prom.GaugeValue,
					values[i].Value,
				)
			case "total":
				continue
			default:
				ch <- prom.MustNewConstMetric(
					prom.NewDesc(
						prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "buffer_pool_page_changes_total"),
						"Innodb buffer pool page state changes.",
						[]string{"operation"}, nil,
					),
					prom.CounterValue,
					values[i].Value,
					match[2],
				)
			}
		case "innodb_rows":
			ch <- prom.MustNewConstMetric(
				prom.NewDesc(
					prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "innodb_row_ops_total"),
					"Total number of MySQL InnoDB row operations.",
					[]string{"operation"}, nil,
				),
				prom.CounterValue,
				values[i].Value,
				match[2],
			)
		case "performance_schema":
			ch <- prom.MustNewConstMetric(
				prom.NewDesc(
					prom.BuildFQName(GENERIC_PREFIX, tr.Domain, "performance_schema_lost_total"),
					"Total number of MySQL instrumentations that could not be loaded or created due to memory constraints.",
					[]string{"instrumentation"}, nil,
				),
				prom.CounterValue,
				values[i].Value,
				match[2],
			)
		}
	}
}
