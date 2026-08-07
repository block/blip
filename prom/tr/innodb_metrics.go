// Copyright 2024 Block, Inc.

package tr

import (
	"regexp"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/cashapp/blip/v2"
)

type InnoDBMetrics struct {
	Domain      string
	ShortDomain string
}

func (tr InnoDBMetrics) Names() (string, string, string) {
	return GENERIC_PREFIX, tr.Domain, tr.ShortDomain
}

// Copied from /percona/mysqld_exporter/collector/info_schema_innodb_metrics.go

// Metrics descriptors.
var (
	infoSchemaBufferPageReadTotalDesc = prom.NewDesc(
		prom.BuildFQName("mysql", "info_schema", "innodb_metrics_buffer_page_read_total"),
		"Total number of buffer pages read total.",
		[]string{"type"}, nil,
	)
	infoSchemaBufferPageWrittenTotalDesc = prom.NewDesc(
		prom.BuildFQName("mysql", "info_schema", "innodb_metrics_buffer_page_written_total"),
		"Total number of buffer pages written total.",
		[]string{"type"}, nil,
	)
	infoSchemaBufferPoolPagesDesc = prom.NewDesc(
		prom.BuildFQName("mysql", "info_schema", "innodb_metrics_buffer_pool_pages"),
		"Total number of buffer pool pages by state.",
		[]string{"state"}, nil,
	)
	infoSchemaBufferPoolPagesDirtyDesc = prom.NewDesc(
		prom.BuildFQName("mysql", "info_schema", "innodb_metrics_buffer_pool_dirty_pages"),
		"Total number of dirty pages in the buffer pool.",
		nil, nil,
	)
)

// Regexp for matching metric aggregations.
var (
	bufferRE     = regexp.MustCompile(`^buffer_(pool_pages)_(.*)$`)
	bufferPageRE = regexp.MustCompile(`^buffer_page_(read|written)_(.*)$`)
)

func (tr InnoDBMetrics) Translate(values []blip.MetricValue, ch chan<- prom.Metric) {
	for i := range values {
		subsystem, ok := values[i].Meta["subsystem"]
		if !ok {
			continue
		}

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

		// Special handling of the "buffer_page_io" subsystem.
		if subsystem == "buffer_page_io" {
			match := bufferPageRE.FindStringSubmatch(values[i].Name)
			if len(match) != 3 {
				//level.Warn(logger).Log("msg", "innodb_metrics subsystem buffer_page_io returned an invalid name", "name", name)
				continue
			}
			switch match[1] {
			case "read":
				ch <- prom.MustNewConstMetric(
					infoSchemaBufferPageReadTotalDesc, promType, values[i].Value, match[2],
				)
			case "written":
				ch <- prom.MustNewConstMetric(
					infoSchemaBufferPageWrittenTotalDesc, promType, values[i].Value, match[2],
				)
			}
			continue
		}
		if subsystem == "buffer" {
			match := bufferRE.FindStringSubmatch(values[i].Name)
			// Many buffer subsystem metrics are not matched, fall through to generic metric.
			if match != nil {
				switch match[1] {
				case "pool_pages":
					switch match[2] {
					case "total":
						// Ignore total, it is an aggregation of the rest.
						continue
					case "dirty":
						// Dirty pages are a separate metric, not in the total.
						ch <- prom.MustNewConstMetric(
							infoSchemaBufferPoolPagesDirtyDesc, prom.GaugeValue, values[i].Value,
						)
					default:
						ch <- prom.MustNewConstMetric(
							infoSchemaBufferPoolPagesDesc, prom.GaugeValue, values[i].Value, match[2],
						)
					}
				}
				continue
			}
		}

		metricName := "innodb_metrics_" + subsystem + "_" + values[i].Name
		description := prom.NewDesc(
			prom.BuildFQName("mysql", "info_schema", metricName+"_total"),
			help, nil, nil,
		)
		ch <- prom.MustNewConstMetric(
			description,
			promType,
			values[i].Value,
		)
	}
}
