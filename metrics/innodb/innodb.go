// Copyright 2024 Block, Inc.

package innodb

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "innodb"

	OPT_ALL = "all"
)

/*
	mysql> SELECT * FROM innodb_metrics WHERE name='trx_rseg_history_len' LIMIT 1\G
	*************************** 1. row ***************************
	           NAME: trx_rseg_history_len
	      SUBSYSTEM: transaction
	          COUNT: 0
	      MAX_COUNT: 0
	      MIN_COUNT: 0
	      AVG_COUNT: NULL
	    COUNT_RESET: 0
	MAX_COUNT_RESET: 0
	MIN_COUNT_RESET: 0
	AVG_COUNT_RESET: NULL
	   TIME_ENABLED: 2021-08-17 08:24:14
	  TIME_DISABLED: NULL
	   TIME_ELAPSED: 1905927
	     TIME_RESET: NULL
	         STATUS: enabled
	           TYPE: value
	        COMMENT: Length of the TRX_RSEG_HISTORY list
*/

// InnoDB collects metrics for the innodb domain. The source is
// information_schema.innodb_metrics.
type InnoDB struct {
	db     *sql.DB
	query  map[string]string
	params map[string][]interface{}
}

var _ blip.Collector = &InnoDB{}

func NewInnoDB(db *sql.DB) *InnoDB {
	return &InnoDB{
		db:     db,
		query:  map[string]string{},
		params: map[string][]interface{}{},
	}
}

func (c *InnoDB) Domain() string {
	return DOMAIN
}

func (c *InnoDB) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "InnoDB metrics from information_schema.innodb_metrics like 'trx_rseg_history_len' (HLL)",
		Options: map[string]blip.CollectorHelpOption{
			OPT_ALL: {
				Name:    OPT_ALL,
				Desc:    "Collect all metrics",
				Default: "no",
				Values: map[string]string{
					"yes":     "All metrics (ignore metrics list)",
					"enabled": "Enabled metrics (ignore metrics list)",
					"no":      "Specified metrics",
				},
			},
		},
		Meta: []blip.CollectorKeyValue{
			{Key: "subsystem", Value: "innodb_metrics.subsystem column"},
		},
	}
}

const baseQuery = "SELECT subsystem, name, count FROM information_schema.innodb_metrics"

func (c *InnoDB) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}

		all := strings.ToLower(dom.Options[OPT_ALL])
		switch all {
		case "all":
			c.query[level.Name] = baseQuery
			c.params[level.Name] = []interface{}{}
		case "enabled":
			c.query[level.Name] = baseQuery + " WHERE status='enabled'"
			c.params[level.Name] = []interface{}{}
		default:
			c.query[level.Name] = baseQuery + " WHERE name IN (" + sqlutil.PlaceholderList(len(dom.Metrics)) + ")"
			c.params[level.Name] = sqlutil.ToInterfaceArray(dom.Metrics)
		}
		blip.Debug("%s: innodb metrics at %s: %s", plan.MonitorId, level.Name, c.query[level.Name])
	}
	return nil, nil
}

func (c *InnoDB) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	rows, err := c.db.QueryContext(ctx, c.query[levelName], c.params[levelName]...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metrics := []blip.MetricValue{}

	var (
		subsystem string
		name      string
		val       string
		ok        bool
	)
	for rows.Next() {
		if err = rows.Scan(&subsystem, &name, &val); err != nil {
			return nil, err
		}

		// At first glance, innodb_metrics.name seems lowercase, but it uppercases
		// some acronyms but not all: buffer_LRU_batch_scanned vs. log_lsn_current
		// (LSN = log sequence number). Must strings.ToLower.
		m := blip.MetricValue{
			Name: strings.ToLower(name),
			Type: blip.CUMULATIVE_COUNTER,
			Meta: map[string]string{"subsystem": subsystem},
		}
		if gauge[m.Name] {
			m.Type = blip.GAUGE
		}

		m.Value, ok = sqlutil.Float64(val)
		if !ok {
			blip.Debug("innodb: cannot convert %v = %v", name, val)
			continue
		}

		// Fixed as of 8.0.17: http://bugs.mysql.com/bug.php?id=75966
		if m.Value < 0 {
			m.Value = 0
		}

		metrics = append(metrics, m)
	}

	return metrics, nil
}

var gauge = map[string]bool{
	"buffer_pool_bytes_data":         true,
	"buffer_pool_bytes_dirty":        true,
	"buffer_pool_pages_data":         true,
	"buffer_pool_pages_dirty":        true,
	"buffer_pool_pages_free":         true,
	"buffer_pool_pages_misc":         true,
	"buffer_pool_pages_total":        true,
	"buffer_pool_size":               true,
	"ddl_pending_alter_table":        true,
	"file_num_open_files":            true,
	"innodb_page_size":               true,
	"lock_row_lock_time_avg":         true,
	"lock_row_lock_time_max":         true,
	"lock_threads_waiting":           true,
	"log_lsn_archived":               true,
	"log_lsn_buf_dirty_pages_added":  true,
	"log_lsn_buf_pool_oldest_approx": true,
	"log_lsn_buf_pool_oldest_lwm":    true,
	"log_lsn_checkpoint_age":         true,
	"log_lsn_current":                true,
	"log_lsn_last_checkpoint":        true,
	"log_lsn_last_flush":             true,
	"log_max_modified_age_async":     true,
	"log_max_modified_age_sync":      true,
	"os_log_pending_fsyncs":          true,
	"os_log_pending_writes":          true,
	"os_pending_reads":               true,
	"os_pending_writes":              true,
	"purge_dml_delay_usec":           true,
	"purge_resume_count":             true,
	"purge_stop_count":               true,
	"lock_row_lock_current_waits":    true,
	"trx_active_transactions":        true, // counter according to i_s.innodb_metrics.comment
	"trx_rseg_current_size":          true, // rseg size in pages
	"trx_rseg_history_len":           true, // history list length
}
