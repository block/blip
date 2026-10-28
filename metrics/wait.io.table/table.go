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
	OPT_GROUP_BY         = "group-by"

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
	schema            bool
	truncate          bool
	truncateTimeout   time.Duration
	stop              bool
	truncateErrPolicy *errors.TruncateErrorPolicy
	lockWaitQuery     string
	metricType        byte
}

// skipTruncate tells the truncate error policy that the table was not
// truncated because reading it (collectQuery) failed. Without it, the next
// sample would cover more than one interval but be reported as a one-interval
// delta.
func (o *tableOptions) skipTruncate() {
	if o.truncate {
		o.truncateErrPolicy.SkipTruncate()
	}
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
			OPT_GROUP_BY: {
				Name:    OPT_GROUP_BY,
				Desc:    "Group table I/O by table or owning schema",
				Default: "table",
				Values: map[string]string{
					"table":  "One series per table",
					"schema": "One series per schema",
				},
			},
		},
		Groups: []blip.CollectorKeyValue{
			{Key: "db", Value: "database name for table or schema series"},
			{Key: "tbl", Value: "table name; present only for table series"},
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

		switch dom.Options[OPT_GROUP_BY] {
		case "", "table":
		case "schema":
			o.schema = true
		default:
			return nil, fmt.Errorf("invalid %s value: %q", OPT_GROUP_BY, dom.Options[OPT_GROUP_BY])
		}
		if o.schema {
			o.query, o.params = TableIoWaitSchemaQuery(dom.Options, dom.Metrics)
		} else {
			o.query, o.params = TableIoWaitQuery(dom.Options, dom.Metrics)
		}

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

	metrics, err := t.collectQuery(ctx, o.query, o.params, o.metricType, o.schema)
	if err != nil {
		o.skipTruncate()
		return nil, err
	}

	if o.truncate {
		conn, err := t.db.Conn(ctx)
		if err == nil {
			defer conn.Close()
			// Set lock_wait_timeout so metadata locking cannot stall this collector.
			// If this fails, treat it like a truncate failure rather than risk a long wait.
			_, err = conn.ExecContext(ctx, o.lockWaitQuery)
			if err == nil {
				trCtx, cancelFn := context.WithTimeout(ctx, o.truncateTimeout)
				defer cancelFn()
				_, err = conn.ExecContext(trCtx, TRUNCATE_QUERY)
			}
		}
		// The policy also processes metric values when truncation succeeds.
		return o.truncateErrPolicy.TruncateError(err, &o.stop, metrics)
	}

	return metrics, nil
}

func (t *Table) collectQuery(ctx context.Context, query string, params []interface{}, metricType byte, schema bool) ([]blip.MetricValue, error) {
	rows, err := t.db.QueryContext(ctx, query, params...)
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
		return nil, fmt.Errorf("Failed to get columns for wait.io.table: %w", err)
	}

	values = make([]interface{}, len(cols))
	values[0] = new(string)
	values[1] = new(string)

	for i := 2; i < len(cols); i++ {
		if schema {
			// SUM of unsigned table counters can exceed int64, and schema
			// metrics are represented as float64 after scanning.
			values[i] = new(float64)
		} else {
			values[i] = new(int64)
		}
	}

	for rows.Next() {
		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		dbName = *values[0].(*string)
		tblName = *values[1].(*string)

		for i := 2; i < len(cols); i++ {
			group := map[string]string{"db": dbName, "tbl": tblName}
			if schema {
				group = map[string]string{"db": dbName}
			}
			m := blip.MetricValue{
				Name:  cols[i],
				Type:  metricType,
				Group: group,
			}
			if schema {
				m.Value = *values[i].(*float64)
			} else {
				m.Value = float64(*values[i].(*int64))
			}
			metrics = append(metrics, m)
		}

	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Failed to read rows for wait.io.table: %w", err)
	}

	return metrics, nil
}
