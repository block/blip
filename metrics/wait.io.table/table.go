// Copyright 2024 Block, Inc.

package waitiotable

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
)

const (
	DOMAIN = "wait.io.table"

	OPT_EXCLUDE          = "exclude"
	OPT_INCLUDE          = "include"
	OPT_TRUNCATE_TABLE   = "truncate-table"
	OPT_TRUNCATE_TIMEOUT = "truncate-timeout"
	OPT_ALL              = "all"

	OPT_EXCLUDE_DEFAULT = "mysql.*,information_schema.*,performance_schema.*,sys.*"

	TRUNCATE_QUERY = "TRUNCATE TABLE performance_schema.table_io_waits_summary_by_table"

	ERR_TRUNCATE_FAILED = "truncate-timeout"
	LOCKWAIT_QUERY      = "SET @@session.lock_wait_timeout=%d"
)

var (
	columnNames = []string{
		"count_star",
		"sum_timer_wait",
		"min_timer_wait",
		"avg_timer_wait",
		"max_timer_wait",
		"count_read",
		"sum_timer_read",
		"min_timer_read",
		"avg_timer_read",
		"max_timer_read",
		"count_write",
		"sum_timer_write",
		"min_timer_write",
		"avg_timer_write",
		"max_timer_write",
		"count_fetch",
		"sum_timer_fetch",
		"min_timer_fetch",
		"avg_timer_fetch",
		"max_timer_fetch",
		"count_insert",
		"sum_timer_insert",
		"min_timer_insert",
		"avg_timer_insert",
		"max_timer_insert",
		"count_update",
		"sum_timer_update",
		"min_timer_update",
		"avg_timer_update",
		"max_timer_update",
		"count_delete",
		"sum_timer_delete",
		"min_timer_delete",
		"avg_timer_delete",
		"max_timer_delete",
	}

	columnExists map[string]struct{}
)

func init() {
	columnExists = make(map[string]struct{}, len(columnNames))
	for _, name := range columnNames {
		columnExists[name] = struct{}{}
	}
}

type tableOptions struct {
	query             string
	params            []interface{}
	truncate          bool
	truncateTimeout   time.Duration
	stop              bool
	truncateErrPolicy *errors.TruncateErrorPolicy
	lockWaitQuery     string
	metricType        byte
}

// Table collects table io for domain wait.io.table.
type Table struct {
	db *sql.DB
	// --
	options map[string]*tableOptions
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &Table{}

// NewTable makes a new Table collector,
func NewTable(db *sql.DB) *Table {
	return &Table{
		db:      db,
		options: map[string]*tableOptions{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *Table) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (t *Table) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Table IO Waits",
		Options: map[string]blip.CollectorHelpOption{
			OPT_INCLUDE: {
				Name: OPT_INCLUDE,
				Desc: "Comma-separated list of database or table names to include (overrides option " + OPT_EXCLUDE + ")",
			},
			OPT_EXCLUDE: {
				Name:    OPT_EXCLUDE,
				Desc:    "Comma-separated list of database or table names to exclude (ignored if " + OPT_INCLUDE + " is set)",
				Default: OPT_EXCLUDE_DEFAULT,
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
			OPT_ALL: {
				Name:    OPT_ALL,
				Desc:    "Collect all metrics",
				Default: "no",
				Values: map[string]string{
					"yes": "All metrics (ignore metrics list)",
					"no":  "Specified metrics",
				},
			},
		},
		Groups: []blip.CollectorKeyValue{
			{Key: "db", Value: "the database name for the corresponding table io, or empty string for all dbs"},
			{Key: "tbl", Value: "the table name for the corresponding table io, or empty string for all tables"},
		},
		Errors: map[string]blip.CollectorHelpError{
			ERR_TRUNCATE_FAILED: {
				Name:    ERR_TRUNCATE_FAILED,
				Handles: "Truncation failures on 'performance_schema.table_io_waits_summary_by_table'",
				Default: errors.NewPolicy("").String(),
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (t *Table) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		o := tableOptions{}

		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected in this level
		}
		if dom.Options == nil {
			dom.Options = make(map[string]string)
		}
		if _, ok := dom.Options[OPT_EXCLUDE]; !ok {
			dom.Options[OPT_EXCLUDE] = OPT_EXCLUDE_DEFAULT
		}

		o.query, o.params = TableIoWaitQuery(dom.Options, dom.Metrics)

		if truncate, ok := dom.Options[OPT_TRUNCATE_TABLE]; ok && truncate == "no" {
			o.truncate = false
			o.metricType = blip.CUMULATIVE_COUNTER
		} else {
			o.truncate = true // default
			o.metricType = blip.DELTA_COUNTER
		}

		if truncateTimeout, ok := dom.Options[OPT_TRUNCATE_TIMEOUT]; ok && o.truncate {
			if duration, err := time.ParseDuration(truncateTimeout); err != nil {
				return nil, fmt.Errorf("Invalid truncate duration: %v", err)
			} else {
				o.truncateTimeout = duration
			}
		} else {
			o.truncateTimeout = 250 * time.Millisecond // default
		}

		if o.truncate {
			// Setup our lock wait timeout. It needs to be at least as long
			// as our truncate timeout, but the granularity of the lock wait
			// timeout is seconds, so we round up to the nearest second that is
			// greater than our truncate timeout.
			lockWaitTimeout := math.Ceil(o.truncateTimeout.Seconds())
			if lockWaitTimeout < 1.0 {
				lockWaitTimeout = 1
			}

			o.lockWaitQuery = fmt.Sprintf(LOCKWAIT_QUERY, int64(lockWaitTimeout))
			o.truncateErrPolicy = errors.NewTruncateErrorPolicy(dom.Errors[ERR_TRUNCATE_FAILED])
			blip.Debug("error policy: %s=%s", ERR_TRUNCATE_FAILED, o.truncateErrPolicy.Policy)
		}

		t.options[level.Name] = &o
	}
	return nil, nil
}

func (t *Table) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	o, ok := t.options[levelName]
	if !ok {
		return nil, nil
	}

	if o.stop {
		blip.Debug("stopped by previous error")
		return nil, nil
	}

	rows, err := t.db.QueryContext(ctx, o.query, o.params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		metrics []blip.MetricValue
		dbName  string
		tblName string
		values  []interface{}
	)

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Failed to get columns for wait.io.table: %v", err)
	}

	values = make([]interface{}, len(cols))
	values[0] = new(string)
	values[1] = new(string)

	for i := 2; i < len(cols); i++ {
		values[i] = new(int64)
	}

	for rows.Next() {
		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		dbName = *values[0].(*string)
		tblName = *values[1].(*string)

		for i := 2; i < len(cols); i++ {
			m := blip.MetricValue{
				Name:  cols[i],
				Type:  o.metricType,
				Group: map[string]string{"db": dbName, "tbl": tblName},
			}
			m.Value = float64(*values[i].(*int64))
			metrics = append(metrics, m)
		}

	}

	if o.truncate {
		conn, err := t.db.Conn(ctx)
		if err == nil {
			defer conn.Close()

			// Set `lock_wait_timeout` to prevent our query from being blocked for too long
			// due to metadata locking. We treat a failure to set the lock wait timeout
			// the same as a truncate timeout, as not setting creates a risk of having a thread
			// hang for an extended period of time.
			_, err = conn.ExecContext(ctx, o.lockWaitQuery)
			if err == nil {
				trCtx, cancelFn := context.WithTimeout(ctx, o.truncateTimeout)
				defer cancelFn()
				_, err = conn.ExecContext(trCtx, TRUNCATE_QUERY)
			}
		}
		// Process any errors (or lack thereof) with the TruncateErrorPolicy as there is special handling
		// for the metric values that need to be applied, even if there is not an error. See comments
		// in `TruncateErrorPolicy` for more details.
		return o.truncateErrPolicy.TruncateError(err, &o.stop, metrics)
	}

	return metrics, err
}
