// Copyright 2024 Block, Inc.

package queryresponsetime

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	myerr "github.com/go-mysql/errors"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "query.response-time"

	OPT_REAL_PERCENTILES = "real-percentiles"
	OPT_TRUNCATE_TABLE   = "truncate-table"
	OPT_TRUNCATE_TIMEOUT = "truncate-timeout"

	defaultPercentile = "p999"

	ERR_NO_TABLE        = "table-not-exist"
	ERR_TRUNCATE_FAILED = "truncate-timeout"

	BASE_QUERY     = "SELECT ROUND(bucket_quantile * 100, 1) AS p, ROUND(bucket_timer_high / 1000000, 3) AS us FROM performance_schema.events_statements_histogram_global"
	TRUNCATE_QUERY = "TRUNCATE TABLE performance_schema.events_statements_histogram_global"
	LOCKWAIT_QUERY = "SET @@session.lock_wait_timeout=%d"
)

type percentile struct {
	formatted string // p95
	query     string
}

type qrtConfig struct {
	percentiles       []percentile
	setMeta           bool
	truncate          bool
	truncateTimeout   time.Duration
	stop              bool
	errPolicy         map[string]*errors.Policy
	truncateErrPolicy *errors.TruncateErrorPolicy
	lockWaitQuery     string
}

type ResponseTime struct {
	db *sql.DB
	// --
	atLevel map[string]*qrtConfig // keyed on level
}

var _ blip.Collector = &ResponseTime{}

func NewResponseTime(db *sql.DB) *ResponseTime {
	return &ResponseTime{
		db:      db,
		atLevel: map[string]*qrtConfig{},
	}
}

func percentileMetrics(metrics []string) ([]sqlutil.P, error) {
	if len(metrics) == 0 {
		metrics = []string{defaultPercentile}
	}
	for _, metric := range metrics {
		if len(metric) < 2 || (metric[0] != 'p' && metric[0] != 'P') {
			return nil, fmt.Errorf("invalid percentile metric %q: expected pN where N is an integer from 1 through 999", metric)
		}
		n, err := strconv.Atoi(metric[1:])
		if err != nil || n < 1 || n > 999 {
			return nil, fmt.Errorf("invalid percentile metric %q: expected pN where N is an integer from 1 through 999", metric)
		}
	}
	return sqlutil.PercentileMetrics(metrics)
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (c *ResponseTime) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (c *ResponseTime) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Collect metrics for query response time",
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
			OPT_TRUNCATE_TABLE: {
				Name:    OPT_TRUNCATE_TABLE,
				Desc:    "If the source table should be truncated to reset data after each retrieval",
				Default: "yes",
				Values: map[string]string{
					"yes": "Truncate source table after each retrieval",
					"no":  "Do not truncate source table after each retrieval",
				},
			},
			OPT_TRUNCATE_TIMEOUT: {
				Name:    OPT_TRUNCATE_TIMEOUT,
				Desc:    "The amount of time to attempt to truncate the source table before timing out",
				Default: "250ms",
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
			ERR_NO_TABLE: {
				Name:    ERR_NO_TABLE,
				Handles: "MySQL error 1146: Table 'performance_schema.events_statements_histogram_global' doesn't exist",
				Default: errors.NewPolicy("").String(),
			},
			ERR_TRUNCATE_FAILED: {
				Name:    ERR_TRUNCATE_FAILED,
				Handles: "Truncation failures on table 'performance_schema.events_statements_histogram_global'",
				Default: errors.NewPolicy("").String(),
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (c *ResponseTime) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}

		config := &qrtConfig{}
		if rp, ok := dom.Options[OPT_REAL_PERCENTILES]; ok && rp == "no" {
			config.setMeta = false
		} else {
			config.setMeta = true // default
		}

		if truncate, ok := dom.Options[OPT_TRUNCATE_TABLE]; ok && truncate == "no" {
			config.truncate = false
		} else {
			config.truncate = true // default
		}

		if truncateTimeout, ok := dom.Options[OPT_TRUNCATE_TIMEOUT]; ok && config.truncate {
			if duration, err := time.ParseDuration(truncateTimeout); err != nil {
				return nil, fmt.Errorf("Invalid truncate duration: %v", err)
			} else {
				config.truncateTimeout = duration
			}
		} else {
			config.truncateTimeout = 250 * time.Millisecond // default
		}

		if config.truncate {
			// Setup our lock wait timeout. It needs to be at least as long
			// as our truncate timeout, but the granularity of the lock wait
			// timeout is seconds, so we round up to the nearest second that is
			// greater than our truncate timeout.
			lockWaitTimeout := math.Ceil(config.truncateTimeout.Seconds())
			if lockWaitTimeout < 1.0 {
				lockWaitTimeout = 1
			}

			config.lockWaitQuery = fmt.Sprintf(LOCKWAIT_QUERY, int64(lockWaitTimeout))
		}

		// Process the configured percentile metrics into names and values,
		// applying and validating the documented default during preparation.
		p, err := percentileMetrics(dom.Metrics)
		if err != nil {
			return nil, err
		}

		// For each percentile, save a query to fetch its (closest) value
		config.percentiles = make([]percentile, len(p))
		for i := range p {
			config.percentiles[i] = percentile{
				formatted: p[i].Name,
				query:     BASE_QUERY + fmt.Sprintf(" WHERE bucket_quantile >= %f ORDER BY bucket_number LIMIT 1", p[i].Value),
			}
		}

		// Apply custom error policies, if any
		config.errPolicy = map[string]*errors.Policy{}
		config.errPolicy[ERR_NO_TABLE] = errors.NewPolicy(dom.Errors[ERR_NO_TABLE])
		blip.Debug("error policy: %s=%s", ERR_NO_TABLE, config.errPolicy[ERR_NO_TABLE])

		if config.truncate {
			config.truncateErrPolicy = errors.NewTruncateErrorPolicy(dom.Errors[ERR_TRUNCATE_FAILED])
			blip.Debug("error policy: %s=%s", ERR_TRUNCATE_FAILED, config.truncateErrPolicy.Policy)
		}

		c.atLevel[level.Name] = config
	}

	return nil, nil
}

// Collect collects metrics at the given level.
func (c *ResponseTime) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	if c.atLevel[levelName].stop {
		blip.Debug("stopped by previous error")
		return nil, nil
	}

	var metrics []blip.MetricValue
	for _, percentile := range c.atLevel[levelName].percentiles {
		var p float64
		var us float64
		err := c.db.QueryRowContext(ctx, percentile.query).Scan(&p, &us)
		if err != nil {
			return c.collectError(err, levelName, percentile.formatted)
		}

		m := blip.MetricValue{
			Type:  blip.GAUGE,
			Name:  percentile.formatted,
			Value: us,
		}
		if c.atLevel[levelName].setMeta {
			m.Meta = map[string]string{
				percentile.formatted: fmt.Sprintf("%.1f", p),
			}
		}
		metrics = append(metrics, m)

		blip.Debug("[%s]: Formated percentile value: %s=%f", DOMAIN, percentile.formatted, us)
	}

	// If debugging is turned on dump the raw values from performance_schema.events_statements_histogram_global
	if blip.Debugging {
		var sb strings.Builder

		sb.WriteString("Bucket Number|Bucket Timer Low|Bucket Timer High|Count Bucket|Count Bucket and Lower|Bucket Quantile\n")
		rows, err := c.db.QueryContext(ctx, "SELECT * FROM performance_schema.events_statements_histogram_global ORDER BY bucket_quantile")
		if err == nil {
			defer rows.Close()

			for rows.Next() {
				var bNum int
				var btLow, btHigh, cb, cbL int64
				var quantile float64

				if err := rows.Scan(&bNum, &btLow, &btHigh, &cb, &cbL, &quantile); err == nil {
					sb.WriteString(fmt.Sprintf("%v|%v|%v|%v|%v|%v\n", bNum, btLow, btHigh, cb, cbL, quantile))
				}
			}
		}

		blip.Debug(sb.String())
	}

	if c.atLevel[levelName].truncate {
		conn, err := c.db.Conn(ctx)
		if err == nil {
			defer conn.Close()

			// Set `lock_wait_timeout` to prevent our query from begin blocked for too long
			// due to metadata locking. We treat a failure to set the lock wait timeout
			// the same as a truncate timeout, as not setting creates a risk of having a thread
			// hang for an extended period of time.
			_, err = conn.ExecContext(ctx, c.atLevel[levelName].lockWaitQuery)
			if err == nil {
				trCtx, cancelFn := context.WithTimeout(ctx, c.atLevel[levelName].truncateTimeout)
				defer cancelFn()
				_, err = conn.ExecContext(trCtx, TRUNCATE_QUERY)
			}
		}

		// Process any errors (or lack thereof) with the TruncateErrorPolicy as there is special handling
		// for the metric values that need to be applied, even if there is not an error. See comments
		// in `TruncateErrorPolicy` for more details.
		return c.atLevel[levelName].truncateErrPolicy.TruncateError(err, &c.atLevel[levelName].stop, metrics)
	}

	return metrics, nil
}

func (c *ResponseTime) collectError(err error, levelName string, metricName string) ([]blip.MetricValue, error) {
	var ep *errors.Policy
	switch myerr.MySQLErrorCode(err) {
	case 1146:
		ep = c.atLevel[levelName].errPolicy[ERR_NO_TABLE]
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
		metrics = []blip.MetricValue{{
			Type:  blip.GAUGE,
			Name:  metricName,
			Value: 0,
		}}
	}

	return metrics, reportedErr
}
