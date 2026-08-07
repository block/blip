// Copyright 2024 Block, Inc.

package sizebinlog

import (
	"context"
	"database/sql"

	myerr "github.com/go-mysql/errors"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "size.binlog"

	// No options

	ERR_NO_ACCESS  = "access-denied"
	ERR_NO_BINLOGS = "binlog-not-enabled"
)

// Binlog collects metrics for the size.binlog domain. The source is SHOW BINARY LOGS.
type Binlog struct {
	db *sql.DB
	// --
	cols3     bool
	errPolicy map[string]*errors.Policy
	stop      bool
}

var _ blip.Collector = &Binlog{}

func NewBinlog(db *sql.DB) *Binlog {
	return &Binlog{
		db: db,
		// --
		errPolicy: map[string]*errors.Policy{},
	}
}

func (c *Binlog) Domain() string {
	return DOMAIN
}

func (c *Binlog) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Total size of all binary logs in bytes",
		Errors: map[string]blip.CollectorHelpError{
			ERR_NO_ACCESS: {
				Name:    ERR_NO_ACCESS,
				Handles: "MySQL error 1227: access denied on 'SHOW BINARY LOGS' (need REPLICATION CLIENT priv)",
				Default: errors.NewPolicy("").String(), // defautl EAP
			},
			ERR_NO_BINLOGS: {
				Name:    ERR_NO_BINLOGS,
				Handles: "MySQL error 1381: binary logging not enabled",
				Default: errors.NewPolicy("").String(), // defautl EAP
			},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "bytes",
				Type: blip.GAUGE,
				Desc: "Total size of all binary logs in bytes",
			},
		},
	}
}

func (c *Binlog) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	// Only need to prepare once because nothing changes: the only command is
	// SHOW BINARY LOGS. Look for size.binlog at any level and prepare if set.
	atLevel := ""
	for _, level := range plan.Levels {
		if _, ok := level.Collect[DOMAIN]; ok {
			atLevel = level.Name
			break
		}
	}
	if atLevel == "" {
		return nil, nil // plan does not collect size.binlog at any level
	}

	// ----------------------------------------------------------------------
	// At least one level collect size.binlog, so prepare this collector

	dom := plan.Levels[atLevel].Collect[DOMAIN] // domain at which size.binlog is collected

	// As of MySQL 8.0.14, SHOW BINARY LOGS has 3 cols instead of 2
	if ok, _ := sqlutil.MySQLVersionGTE("8.0.14", c.db, ctx); ok {
		c.cols3 = true
	}

	// Apply custom error policies, if any
	c.errPolicy[ERR_NO_ACCESS] = errors.NewPolicy(dom.Errors[ERR_NO_ACCESS])
	c.errPolicy[ERR_NO_BINLOGS] = errors.NewPolicy(dom.Errors[ERR_NO_BINLOGS])
	blip.Debug("error poliy: %s=%s %s=%s", ERR_NO_ACCESS, c.errPolicy[ERR_NO_ACCESS], ERR_NO_BINLOGS, c.errPolicy[ERR_NO_BINLOGS])

	return nil, nil
}

func (c *Binlog) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	if c.stop {
		blip.Debug("stopped by previous error")
		return nil, nil
	}

	rows, err := c.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return c.collectError(err)
	}
	defer rows.Close()

	var (
		name  string
		val   string
		enc   string
		ok    bool
		n     float64
		total float64
	)
	for rows.Next() {
		if c.cols3 {
			err = rows.Scan(&name, &val, &enc) // 8.0.14+
		} else {
			err = rows.Scan(&name, &val)
		}
		if err != nil {
			return nil, err
		}
		n, ok = sqlutil.Float64(val)
		if !ok {
			continue
		}
		total += n
	}

	metrics := []blip.MetricValue{{
		Name:  "bytes",
		Value: total,
		Type:  blip.GAUGE,
	}}

	return metrics, nil
}

func (c *Binlog) collectError(err error) ([]blip.MetricValue, error) {
	var ep *errors.Policy
	switch myerr.MySQLErrorCode(err) {
	case 1381:
		ep = c.errPolicy[ERR_NO_BINLOGS]
	case 1227:
		ep = c.errPolicy[ERR_NO_ACCESS]
	default:
		return nil, err
	}

	// Stop trying to collect if error policy retry="stop". This affects
	// future calls to Collect; don't retrun yet because we need to check
	// the metric policy: drop or zero. If zero, we must report one zero val.
	if ep.Retry == errors.POLICY_RETRY_NO {
		c.stop = true
	}

	// Report
	var reportedErr error
	if ep.ReportError() {
		reportedErr = err
	} else {
		blip.Debug("error policy=ignore: %s", err)
	}

	var metrics []blip.MetricValue
	if ep.Metric == errors.POLICY_METRIC_ZERO {
		metrics = []blip.MetricValue{{
			Name:  "bytes",
			Value: 0,
			Type:  blip.GAUGE,
		}}
	}

	return metrics, reportedErr
}
