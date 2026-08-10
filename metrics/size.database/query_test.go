// Copyright 2024 Block, Inc.

package sizedatabase_test

import (
	"testing"

	sizedatabase "github.com/cashapp/blip/v2/metrics/size.database"
	"github.com/go-test/deep"
)

func TestDataSizeQuery(t *testing.T) {
	dataSize := sizedatabase.NewDatabase(nil)

	// All defaults
	opts := map[string]string{}
	got, params, err := sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect := "SELECT table_schema AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema NOT IN (?, ?, ?, ?) GROUP BY 1"
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

	// Exclude specific databases
	opts = map[string]string{
		sizedatabase.OPT_EXCLUDE: "a,b,c",
	}
	got, params, err = sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect = "SELECT table_schema AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema NOT IN (?, ?, ?) GROUP BY 1"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"a", "b", "c"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Include specific databases
	opts = map[string]string{
		sizedatabase.OPT_INCLUDE: "foo,bar",
	}
	got, params, err = sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect = "SELECT table_schema AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema IN (?, ?) GROUP BY 1"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"foo", "bar"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Include overrides exclude
	opts = map[string]string{
		sizedatabase.OPT_INCLUDE: "foo,bar",
		sizedatabase.OPT_EXCLUDE: "a,b,c", // ignored
	}
	got, params, err = sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect = "SELECT table_schema AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema IN (?, ?) GROUP BY 1"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"foo", "bar"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// LIKE include
	opts = map[string]string{
		sizedatabase.OPT_LIKE:    "yes",
		sizedatabase.OPT_INCLUDE: "foo,bar",
	}
	got, params, err = sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect = "SELECT table_schema AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema LIKE ? OR table_schema LIKE ? GROUP BY 1"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"foo", "bar"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// LIKE exclude
	opts = map[string]string{
		sizedatabase.OPT_LIKE:    "yes",
		sizedatabase.OPT_EXCLUDE: "x,y,z",
	}
	got, params, err = sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect = "SELECT table_schema AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema NOT LIKE ? AND table_schema NOT LIKE ? AND table_schema NOT LIKE ? GROUP BY 1"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"x", "y", "z"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Total only (with default exclude)
	opts = map[string]string{
		sizedatabase.OPT_TOTAL: "only",
	}
	got, params, err = sizedatabase.DataSizeQuery(opts, dataSize.Help())
	expect = "SELECT \"\" AS db, SUM(data_length+index_length) AS bytes FROM information_schema.tables WHERE table_schema NOT IN (?, ?, ?, ?)"
	if err != nil {
		t.Error(err)
	}
	if got != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got, expect)
	}

	expectedParams = []interface{}{"mysql", "information_schema", "performance_schema", "sys"}
	if diff := deep.Equal(params, expectedParams); diff != nil {
		t.Error(diff)
	}
}
