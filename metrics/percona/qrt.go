// Copyright 2024 Block, Inc.

package percona

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	myerr "github.com/go-mysql/errors"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
	"github.com/cashapp/blip/v2/sqlutil"
)

/*
Percona root@localhost:(none)> SELECT time, count, total FROM INFORMATION_SCHEMA.QUERY_RESPONSE_TIME WHERE TIME != 'TOO LONG';\G
+----------------+-------+----------------+
| time           | count | total          |
+----------------+-------+----------------+
|       0.000001 | 0     |       0.000000 |
|       0.000010 | 0     |       0.000000 |
|       0.000100 | 0     |       0.000000 |
|       0.001000 | 0     |       0.000000 |
|       0.010000 | 0     |       0.000000 |
|       0.100000 | 0     |       0.000000 |
|       1.000000 | 0     |       0.000000 |
|      10.000000 | 0     |       0.000000 |
|     100.000000 | 0     |       0.000000 |
|    1000.000000 | 0     |       0.000000 |
|   10000.000000 | 0     |       0.000000 |
|  100000.000000 | 0     |       0.000000 |
| 1000000.000000 | 0     |       0.000000 |
+----------------+-------+----------------+
*/

const (
	blip_domain = "percona.response-time"
)

const (
	OPT_REAL_PERCENTILES = "real-percentiles"
	OPT_FLUSH_QRT        = "flush"

	ERR_UNKNOWN_TABLE = "unknown-table"
)

const (
	query      = "SELECT time, count, total FROM INFORMATION_SCHEMA.QUERY_RESPONSE_TIME WHERE TIME!='TOO LONG';"
	flushQuery = "SET GLOBAL query_response_time_flush=1"
)

type percentile struct {
	p         float64 // 0.95
	formatted string  // p95
}

type qrtConfig struct {
	percentiles []percentile
	setMeta     bool
	flush       bool
	stop        bool
	errPolicy   map[string]*errors.Policy
}

type QRT struct {
	db      *sql.DB
	atLevel map[string]*qrtConfig // keyed on level
}

func NewQRT(db *sql.DB) *QRT {
	return &QRT{
		db:      db,
		atLevel: map[string]*qrtConfig{},
	}
}

func (c *QRT) Domain() string {
	return blip_domain
}

func (c *QRT) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      blip_domain,
		Description: "Collect QRT (Query Response Time) metrics",
		Options: map[string]blip.CollectorHelpOption{
			OPT_REAL_PERCENTILES: {
				Name:    OPT_REAL_PERCENTILES,
				Desc:    "If real percentiles are included in meta",
				Default: "yes",
				Values: map[string]string{
					"yes": "Include real percentiles in meta",
					"no":  "Exclude real percentiles in meta",
				},
			},
			OPT_FLUSH_QRT: {
				Name:    OPT_FLUSH_QRT,
				Desc:    "If Query Response Time should be flushed after each retrieval.",
				Default: "yes",
				Values: map[string]string{
					"yes": "Flush Query Response Time (QRT) after each retrieval.",
					"no":  "Do not flush Query Response Time (QRT) after each retrieval.",
				},
			},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "pN",
				Type: blip.GAUGE,
				Desc: "Percentile to collect where N between 1 and 999 (p99=99th, p999=99.9th)",
			},
		},
		Errors: map[string]blip.CollectorHelpError{
			ERR_UNKNOWN_TABLE: {
				Name:    ERR_UNKNOWN_TABLE,
				Handles: "MySQL error 1109: Unknown table 'query_response_time' in information_schema",
				Default: errors.NewPolicy("").String(),
			},
		},
	}
}

// Prepare Prepares options for all levels in the plan that contain the percona.response-time domain
func (c *QRT) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[blip_domain]
		if !ok {
			continue LEVEL
		}

		config := &qrtConfig{}
		if rp, ok := dom.Options[OPT_REAL_PERCENTILES]; ok && rp == "no" {
			config.setMeta = false
		} else {
			config.setMeta = true // default
		}

		if flushQrt, ok := dom.Options[OPT_FLUSH_QRT]; ok && flushQrt == "no" {
			config.flush = false
		} else {
			config.flush = true // default
		}

		// Process list of percentiles metrics into a list of names and values
		p, err := sqlutil.PercentileMetrics(dom.Metrics)
		if err != nil {
			return nil, err
		}

		// For each percentile, save a query to fetch its (closest) value
		config.percentiles = make([]percentile, len(p))
		for i := range p {
			config.percentiles[i] = percentile{
				p:         p[i].Value,
				formatted: p[i].Name,
			}
		}

		// Apply custom error policies, if any
		config.errPolicy = map[string]*errors.Policy{}
		config.errPolicy[ERR_UNKNOWN_TABLE] = errors.NewPolicy(dom.Errors[ERR_UNKNOWN_TABLE])
		blip.Debug("error policy: %s=%s", ERR_UNKNOWN_TABLE, config.errPolicy[ERR_UNKNOWN_TABLE])

		c.atLevel[level.Name] = config
	}
	return nil, nil
}

// Collect Collects query response time metrics for a particular level
func (c *QRT) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	if c.atLevel[levelName].stop {
		blip.Debug("stopped by previous error")
		return nil, nil
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return c.collectError(err, levelName, c.atLevel[levelName].percentiles)
	}
	defer rows.Close()

	var buckets []QRTBucket

	var time string
	var count uint64
	var total string
	for rows.Next() {
		if err := rows.Scan(&time, &count, &total); err != nil {
			return nil, err
		}

		validatedTime, ok := sqlutil.Float64(strings.TrimSpace(time))
		if !ok {
			return nil, fmt.Errorf("%s: qrt: time could't be parsed into a valid float: %s ", levelName, time)
		}

		validatedTotal, ok := sqlutil.Float64(strings.TrimSpace(total))
		if !ok {
			return nil, fmt.Errorf("%s: qrt: total couldn't be parsed into a valid float: %s ", levelName, total)
		}

		buckets = append(buckets, QRTBucket{Time: validatedTime, Count: count, Total: validatedTotal})
	}

	h := NewQRTHistogram(buckets)

	var metrics []blip.MetricValue
	for _, percentile := range c.atLevel[levelName].percentiles {
		// Get value of percentile (e.g. p999) and actual percentile (e.g. p997).
		// The latter is reported as meta so user can discard percentile if the
		// actual percentile is too far off, which can happen if bucket range is
		// configured too small.
		value, actualPercentile := h.Percentile(percentile.p)
		m := blip.MetricValue{
			Type:  blip.GAUGE,
			Name:  percentile.formatted,
			Value: value * 1000000, // convert seconds to microseconds for consistency with PFS quantiles
		}
		if c.atLevel[levelName].setMeta {
			m.Meta = map[string]string{
				percentile.formatted: fmt.Sprintf("%.1f", actualPercentile*100),
			}
		}
		metrics = append(metrics, m)
	}

	if c.atLevel[levelName].flush {
		_, err = c.db.Exec(flushQuery)
		if err != nil {
			return nil, err
		}
	}

	return metrics, nil
}

func (c *QRT) collectError(err error, levelName string, percentiles []percentile) ([]blip.MetricValue, error) {
	var ep *errors.Policy
	switch myerr.MySQLErrorCode(err) {
	case 1109:
		ep = c.atLevel[levelName].errPolicy[ERR_UNKNOWN_TABLE]
	default:
		return nil, err
	}

	// Stop trying to collect if error policy retry="stop". This affects
	// future calls to Collect; don't return yet because we need to check
	// the metric policy: drop or zero. If zero, we must report one zero val.
	if ep.Retry == errors.POLICY_RETRY_NO {
		c.atLevel[levelName].stop = true
	}

	// Report
	var reportedErr error
	if ep.ReportError() {
		reportedErr = err
	} else {
		blip.Debug("error policy=ignore: %v", err)
	}

	var metrics []blip.MetricValue
	if ep.Metric == errors.POLICY_METRIC_ZERO {
		for _, percentile := range percentiles {
			m := blip.MetricValue{
				Type:  blip.GAUGE,
				Name:  percentile.formatted,
				Value: 0,
			}
			metrics = append(metrics, m)
		}
	}

	return metrics, reportedErr
}
