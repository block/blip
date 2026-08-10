// Copyright 2024 Block, Inc.

package waitiotable_test

import (
	"testing"

	waitiotable "github.com/cashapp/blip/v2/metrics/wait.io.table"
	"github.com/go-test/deep"
)

func TestTableIoQuery(t *testing.T) {
	// All defaults
	opts := map[string]string{
		waitiotable.OPT_EXCLUDE: "mysql.*,information_schema.*,performance_schema.*,sys.*",
		waitiotable.OPT_ALL:     "no",
	}

	metrics := []string{
		"count_fetch",
		"count_insert",
	}

	got, params := waitiotable.TableIoWaitQuery(opts, metrics)
	expect := "SELECT OBJECT_SCHEMA, OBJECT_NAME, count_fetch, count_insert FROM performance_schema.table_io_waits_summary_by_table WHERE NOT (OBJECT_SCHEMA = ?) AND NOT (OBJECT_SCHEMA = ?) AND NOT (OBJECT_SCHEMA = ?) AND NOT (OBJECT_SCHEMA = ?)"
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams := []interface{}{"mysql", "information_schema", "performance_schema", "sys"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Exclude schemas, mysql, and sys
	opts = map[string]string{
		waitiotable.OPT_INCLUDE: "test_table,sys.*,information_schema.XTRADB_ZIP_DICT",
		waitiotable.OPT_ALL:     "no",
	}
	got, params = waitiotable.TableIoWaitQuery(opts, metrics)
	expect = "SELECT OBJECT_SCHEMA, OBJECT_NAME, count_fetch, count_insert FROM performance_schema.table_io_waits_summary_by_table WHERE (OBJECT_NAME = ?) OR (OBJECT_SCHEMA = ?) OR (OBJECT_SCHEMA = ? AND OBJECT_NAME = ?)"
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"test_table", "sys", "information_schema", "XTRADB_ZIP_DICT"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Use the default columns
	opts = map[string]string{
		waitiotable.OPT_INCLUDE: "test_table,sys.*,information_schema.XTRADB_ZIP_DICT",
		waitiotable.OPT_ALL:     "yes",
	}
	got, params = waitiotable.TableIoWaitQuery(opts, []string{})
	expect = "SELECT OBJECT_SCHEMA, OBJECT_NAME, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait, count_read, sum_timer_read, min_timer_read, avg_timer_read, max_timer_read, count_write, sum_timer_write, min_timer_write, avg_timer_write, max_timer_write, count_fetch, sum_timer_fetch, min_timer_fetch, avg_timer_fetch, max_timer_fetch, count_insert, sum_timer_insert, min_timer_insert, avg_timer_insert, max_timer_insert, count_update, sum_timer_update, min_timer_update, avg_timer_update, max_timer_update, count_delete, sum_timer_delete, min_timer_delete, avg_timer_delete, max_timer_delete FROM performance_schema.table_io_waits_summary_by_table WHERE (OBJECT_NAME = ?) OR (OBJECT_SCHEMA = ?) OR (OBJECT_SCHEMA = ? AND OBJECT_NAME = ?)"
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"test_table", "sys", "information_schema", "XTRADB_ZIP_DICT"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}
}
