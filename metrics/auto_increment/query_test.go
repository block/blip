// Copyright 2024 Block, Inc.

package autoincrement_test

import (
	"testing"

	autoincrement "github.com/cashapp/blip/metrics/auto_increment"
	"github.com/go-test/deep"
)

func TestAutoIncrementQuery(t *testing.T) {
	// All defaults
	opts := map[string]string{
		autoincrement.OPT_EXCLUDE: "mysql.*,information_schema.*,performance_schema.*,sys.*",
	}
	got, params, err := autoincrement.AutoIncrementQuery(opts)
	expect := "SELECT table_schema, table_name, column_name, data_type, auto_increment_ratio, is_unsigned FROM sys.schema_auto_increment_columns WHERE auto_increment_ratio IS NOT NULL AND (NOT (table_schema = ?) AND NOT (table_schema = ?) AND NOT (table_schema = ?) AND NOT (table_schema = ?))"
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
		autoincrement.OPT_INCLUDE: "test_table,sys.*,information_schema.XTRADB_ZIP_DICT",
	}
	got, params, err = autoincrement.AutoIncrementQuery(opts)
	expect = "SELECT table_schema, table_name, column_name, data_type, auto_increment_ratio, is_unsigned FROM sys.schema_auto_increment_columns WHERE auto_increment_ratio IS NOT NULL AND ((table_name = ?) OR (table_schema = ?) OR (table_schema = ? AND table_name = ?))"
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
