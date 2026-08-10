// Copyright 2024 Block, Inc.

package sizetable_test

import (
	"testing"

	sizetable "github.com/cashapp/blip/v2/metrics/size.table"
	"github.com/go-test/deep"
)

func TestTableSizeQuery(t *testing.T) {
	// All defaults
	opts := map[string]string{
		sizetable.OPT_EXCLUDE: "mysql.*,information_schema.*,performance_schema.*,sys.*",
	}
	got, params, err := sizetable.TableSizeQuery(opts)
	expect := "SELECT table_schema AS db, table_name as tbl, COALESCE(data_length + index_length, 0) AS tbl_size_bytes FROM information_schema.TABLES WHERE NOT (table_schema = ?) AND NOT (table_schema = ?) AND NOT (table_schema = ?) AND NOT (table_schema = ?)"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams := []interface{}{"mysql", "information_schema", "performance_schema", "sys"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Exclude schemas, mysql, and sys
	opts = map[string]string{
		sizetable.OPT_INCLUDE: "test_table,sys.*,information_schema.XTRADB_ZIP_DICT",
	}
	got, params, err = sizetable.TableSizeQuery(opts)
	expect = "SELECT table_schema AS db, table_name as tbl, COALESCE(data_length + index_length, 0) AS tbl_size_bytes FROM information_schema.TABLES WHERE (table_name = ?) OR (table_schema = ?) OR (table_schema = ? AND table_name = ?)"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"test_table", "sys", "information_schema", "XTRADB_ZIP_DICT"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}
}
