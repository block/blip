// Copyright 2024 Block, Inc.

package waitiotable_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/cashapp/blip/v2"
	waitiotable "github.com/cashapp/blip/v2/metrics/wait.io.table"
	"github.com/cashapp/blip/v2/test"
	_ "github.com/go-sql-driver/mysql"
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

func TestCollectSchemaRollupMySQL80(t *testing.T) {
	_, db, err := test.Connection("mysql80")
	if err != nil {
		var netErr *net.OpError
		if errors.As(err, &netErr) {
			t.Skipf("mysql80 not running: %v", err)
		}
		t.Fatalf("connect to mysql80: %v", err)
	}
	defer db.Close()

	c := waitiotable.NewTable(db)
	plan := blip.Plan{Levels: map[string]blip.Level{
		"test": {Name: "test", Collect: map[string]blip.Domain{
			waitiotable.DOMAIN: {
				Metrics: []string{"count_star", "min_timer_wait", "avg_timer_wait", "max_timer_wait"},
				Options: map[string]string{
					waitiotable.OPT_INCLUDE:        "mysql.*",
					waitiotable.OPT_GROUP_BY:       "both",
					waitiotable.OPT_TRUNCATE_TABLE: "no",
				},
			},
		}},
	}}
	ctx := context.Background()
	if _, err := c.Prepare(ctx, plan); err != nil {
		t.Fatal(err)
	}
	values, err := c.Collect(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	var table, schema bool
	for _, value := range values {
		if value.Group["db"] != "mysql" {
			t.Fatalf("unexpected group: %+v", value.Group)
		}
		if _, ok := value.Group["tbl"]; ok {
			table = true
		} else {
			schema = true
		}
	}
	if !table || !schema {
		t.Fatalf("expected both table and schema metrics, got %d values", len(values))
	}
}

func TestTableIoSchemaQuery(t *testing.T) {
	opts := map[string]string{waitiotable.OPT_INCLUDE: "app.*,other.orders"}
	metrics := []string{"count_fetch", "sum_timer_fetch", "min_timer_fetch", "avg_timer_fetch", "max_timer_fetch"}
	got, params := waitiotable.TableIoWaitSchemaQuery(opts, metrics)
	expect := "SELECT OBJECT_SCHEMA, '' AS OBJECT_NAME, SUM(count_fetch) AS count_fetch, SUM(sum_timer_fetch) AS sum_timer_fetch, COALESCE(MIN(CASE WHEN count_fetch > 0 THEN min_timer_fetch END), 0) AS min_timer_fetch, COALESCE(CAST(SUM(sum_timer_fetch) / NULLIF(SUM(count_fetch), 0) AS UNSIGNED), 0) AS avg_timer_fetch, MAX(max_timer_fetch) AS max_timer_fetch FROM performance_schema.table_io_waits_summary_by_table WHERE (OBJECT_SCHEMA = ?) OR (OBJECT_SCHEMA = ? AND OBJECT_NAME = ?) GROUP BY OBJECT_SCHEMA"
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s", got, expect)
	}
	if diff := deep.Equal(params, []interface{}{"app", "other", "orders"}); diff != nil {
		t.Error(diff)
	}
}
