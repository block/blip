// Copyright 2024 Block, Inc.

package repl

import (
	"context"
	"database/sql"
	"fmt"

	myerr "github.com/go-mysql/errors"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "repl"

	NOT_A_REPLICA = -1

	ERR_NO_ACCESS = "access-denied"

	OPT_REPORT_NOT_A_REPLICA = "report-not-a-replica"
)

type replMetrics struct {
	chedkRunning bool
}

type Repl struct {
	db              *sql.DB
	atLevel         map[string]replMetrics
	errPolicy       map[string]*errors.Policy
	stop            bool
	dropNotAReplica map[string]bool
	statusQuery     string
	newTerms        bool
}

var _ blip.Collector = &Repl{}

func NewRepl(db *sql.DB) *Repl {
	return &Repl{
		db: db,
		// --
		atLevel: map[string]replMetrics{},
		errPolicy: map[string]*errors.Policy{
			ERR_NO_ACCESS: errors.NewPolicy(""),
		},
		dropNotAReplica: map[string]bool{},
		statusQuery:     "SHOW SLAVE STATUS", // SHOW REPLICA STATUS as of 8.022
		newTerms:        false,
	}
}

func (c *Repl) Domain() string {
	return DOMAIN
}

func (c *Repl) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Replication status",
		Options: map[string]blip.CollectorHelpOption{
			OPT_REPORT_NOT_A_REPLICA: {
				Name:    OPT_REPORT_NOT_A_REPLICA,
				Desc:    "Report not a replica as -1",
				Default: "yes",
				Values: map[string]string{
					"yes": "Enabled: report not a replica repl.running = -1",
					"no":  "Disabled: drop repl.running if not a replica",
				},
			},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "running",
				Type: blip.GAUGE,
				Desc: "1=running (no error), 0=not running, -1=not a replica",
			},
		},
		Errors: map[string]blip.CollectorHelpError{
			ERR_NO_ACCESS: {
				Name:    ERR_NO_ACCESS,
				Handles: "MySQL error 1227: access denied on 'SHOW REPLICA STATUS' (need REPLICATION CLIENT priv)",
				Default: c.errPolicy[ERR_NO_ACCESS].String(),
			},
		},
	}
}

func (c *Repl) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	haveVersion := false

LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected in this level
		}

		if len(dom.Metrics) == 0 {
			return nil, fmt.Errorf("no metrics specified, expect at least one collector metric (run 'blip --print-domains' to list collector metrics)")
		}

		m := replMetrics{}
		for i := range dom.Metrics {
			switch dom.Metrics[i] {
			case "running":
				m.chedkRunning = true
			default:
				return nil, fmt.Errorf("invalid collector metric: %s (run 'blip --print-domains' to list collector metrics)", dom.Metrics[i])
			}
		}
		c.atLevel[level.Name] = m
		c.dropNotAReplica[level.Name] = !blip.Bool(dom.Options[OPT_REPORT_NOT_A_REPLICA])

		// Apply custom error policies, if any
		if len(dom.Errors) > 0 {
			if s, ok := dom.Errors[ERR_NO_ACCESS]; ok {
				c.errPolicy[ERR_NO_ACCESS] = errors.NewPolicy(s)
			}
			blip.Debug("error policy: %s=%s", ERR_NO_ACCESS, c.errPolicy[ERR_NO_ACCESS])
		}

		// SHOW REPLICA STATUS as of 8.022
		if haveVersion {
			continue
		}
		major, _, patch := sqlutil.MySQLVersion(ctx, c.db)
		if major == -1 {
			blip.Debug("failed to get/parse MySQL version, ignoring")
			continue
		}
		haveVersion = true
		if major == 8 && patch >= 22 {
			c.statusQuery = "SHOW REPLICA STATUS"
			c.newTerms = true
		}
		blip.Debug("mysql %d.x.%d %s", major, patch, c.statusQuery)
	}
	return nil, nil
}

func (c *Repl) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	if c.stop {
		blip.Debug("stopped by previous error")
		return nil, nil
	}

	rm, ok := c.atLevel[levelName]
	if !ok {
		return nil, nil
	}

	// Return SHOW SLAVE|REPLICA STATUS as map[string]string, which can be nil
	// if MySQL is not a replica
	replStatus, err := sqlutil.RowToMap(ctx, c.db, c.statusQuery)
	if err != nil {
		return c.collectError(err)
	}

	metrics := []blip.MetricValue{}

	// Report repl.running: 1=running, 0=not running, -1=not a replica
	//
	// NOTE: values are literal, not passed through sqlutil.Float64, so
	//       we look for "Yes" not 1, which works in this specific case.
	if rm.chedkRunning {
		var running float64 // 0 = not running by default
		if len(replStatus) == 0 {
			// no SHOW SLAVE|REPLICA STATUS output = not a replica
			running = float64(NOT_A_REPLICA)
		} else if (replStatus["Slave_IO_Running"] == "Yes" || replStatus["Replica_IO_Running"] == "Yes") &&
			(replStatus["Slave_SQL_Running"] == "Yes" || replStatus["Replica_SQL_Running"] == "Yes") && replStatus["Last_Errno"] == "0" {
			// running if a replica and those ^ 3 conditions are true
			running = 1
		}

		if running == NOT_A_REPLICA {
			if c.dropNotAReplica[levelName] {
				return nil, nil
			}
		}

		m := blip.MetricValue{
			Name:  "running",
			Type:  blip.GAUGE,
			Value: running,
			Meta:  map[string]string{"source": ""},
		}
		// Make sure we have results.
		if len(replStatus) != 0 {
			if c.newTerms {
				m.Meta["source"] = replStatus["Source_Host"]
			} else {
				m.Meta["source"] = replStatus["Master_Host"]
			}
		}
		metrics = append(metrics, m)
	}

	// @todo collect other repl status metrics

	return metrics, nil
}

func (c *Repl) collectError(err error) ([]blip.MetricValue, error) {
	var ep *errors.Policy
	switch myerr.MySQLErrorCode(err) {
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
			Name:  "running",
			Type:  blip.GAUGE,
			Value: 0,
			Meta:  map[string]string{"source": ""},
		}}
	}

	return metrics, reportedErr
}
