// Copyright 2024 Block, Inc.

package default_plan

import "github.com/cashapp/blip/v2"

func MySQL() blip.Plan {
	return blip.Plan{
		Name:   "default-mysql",
		Source: "blip",
		Levels: map[string]blip.Level{
			"performance": blip.Level{
				Name: "performance",
				Freq: "5s",
				Collect: map[string]blip.Domain{
					"status.global": {
						Name: "status.global",
						Metrics: []string{
							// Key performance indicators (KPIs)
							"queries",
							"threads_running",

							// Transactions per second (TPS)
							"com_begin",
							"com_commit",
							"com_rollback",

							// Read-write access
							"com_select", // read; the rest are writes
							"com_delete",
							"com_delete_multi",
							"com_insert",
							"com_insert_select",
							"com_replace",
							"com_replace_select",
							"com_update",
							"com_update_multi",

							// Storage IOPS
							"innodb_data_reads",
							"innodb_data_writes",

							// Storage throughput (Bytes/s)
							"innodb_data_written",
							"innodb_data_read",

							// Buffer pool efficiency
							"innodb_buffer_pool_read_requests", // logical reads
							"innodb_buffer_pool_reads",         // disk reads (data not in buffer pool)
							"innodb_buffer_pool_wait_free",     // free page waits

							// Buffer pool usage
							"innodb_buffer_pool_pages_dirty",
							"innodb_buffer_pool_pages_free",
							"innodb_buffer_pool_pages_total",

							// Page flushing
							"innodb_buffer_pool_pages_flushed", // total pages

							// Transaction log throughput (Bytes/s)
							"innodb_os_log_written",
						},
					},
					"innodb": {
						Name: "innodb",
						Metrics: []string{
							// Transactions
							"trx_active_transactions", // (G)

							// Row locking
							"lock_timeouts",
							"lock_row_lock_current_waits", // (G)
							"lock_row_lock_waits",
							"lock_row_lock_time",

							// Page flushing
							"buffer_flush_adaptive_total_pages",   //  adaptive flushing
							"buffer_LRU_batch_flush_total_pages",  //  LRU flushing
							"buffer_flush_background_total_pages", //  legacy flushing

							// Transaction log utilization (%)
							"log_lsn_checkpoint_age",     // checkpoint age
							"log_max_modified_age_async", // async flush point

							// Transaction log -> storage waits
							"innodb_os_log_pending_writes",
							"innodb_log_waits",

							// History List Length (HLL)
							"trx_rseg_history_len",

							// Deadlocks
							"lock_deadlocks",
						},
					},
					"repl": {
						Name: "repl",
						Metrics: []string{
							"running", // -1=not a replica, 0=not running, 1=running ok
						},
						Errors: map[string]string{
							"access-denied": "ignore,drop,retry", // requires SUPER or REPLICATION CLIENT privileges
						},
					},
					"repl.lag": {
						Name: "repl.lag",
						// Automatic; see metrics/repl.lag/lag.go
					},
				},
			}, // level: performance (5s)

			"additional": blip.Level{
				Name: "additional",
				Freq: "20s",
				Collect: map[string]blip.Domain{
					"status.global": {
						Name: "status.global",
						Metrics: []string{
							// Temp objects
							"created_tmp_disk_tables",
							"created_tmp_tables",
							"created_tmp_files",

							// Threads and connections
							"connections",
							"threads_connected", // (G)
							"max_used_connections",

							// Network throughput
							"bytes_sent",
							"bytes_received",

							// Large data changes cached to disk before binlog
							"binlog_cache_disk_use",

							// Prepared statements
							"prepared_stmt_count", // (G)
							"com_stmt_execute",
							"com_stmt_prepare",

							// Client connection errors
							"aborted_clients",
							"aborted_connects",

							// Bad SELECT: should be zero
							"select_full_join",
							"select_full_range_join",
							"select_range_check",
							"select_scan",

							// Admin and SHOW
							"com_flush",
							"com_kill",
							"com_purge",
							"com_admin_commands",
							"com_show_processlist",
							"com_show_slave_status",
							"com_show_status",
							"com_show_variables",
							"com_show_warnings",
						},
					},
				},
			}, // level: additional (20s)

			"data-size": blip.Level{
				Name: "data-size",
				Freq: "5m",
				Collect: map[string]blip.Domain{
					"size.database": {
						Name: "size.database",
						// All databases by default
					},
					"size.table": {
						Name: "size.table",
						// All tables by default
					},
					"size.binlog": {
						Name: "size.binlog",
						// No metrics, there's only one: size.binlog.bytes
						// No options, only collects total binlog size
						Errors: map[string]string{
							"access-denied": "ignore,drop,retry", // requires SUPER or REPLICATION CLIENT privileges
						},
					},
				},
			}, // level: data-size (5m)

			"sysvars": blip.Level{
				Name: "sysvars",
				Freq: "15m",
				Collect: map[string]blip.Domain{
					"var.global": {
						Name: "var.global",
						Metrics: []string{
							"max_connections",
							"max_prepared_stmt_count",
							"innodb_log_file_size",
						},
					},
				},
			}, // level: sysvars (15m)

		},
	}
}
