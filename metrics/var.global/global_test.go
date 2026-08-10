// Copyright 2024 Block, Inc.

package varglobal

import (
	"context"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2/test"
)

func TestPrepareForSingleLevelAndNoSource(t *testing.T) {
	// Given a LevelPlan for a collector with single level and no supplied source,
	// test that Prepare correctly constructs the query for that level (ie: select query)
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewGlobal(db)

	defaultPlan := test.ReadPlan(t, "")
	_, err = c.Prepare(context.Background(), defaultPlan)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t,
		`SELECT CONCAT_WS(',', @@GLOBAL.read_only) v`,
		c.queryIn["kpi"], // level
	)
}

func TestPrepareWithAllSources(t *testing.T) {
	// Given a LevelPlan for a collector with single level and a custom source,
	// test that Prepare correctly constructs the query for that level
	// using that source query.
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	// Test this for all source types ie; auto, select, pfs and show
	queries := map[string]string{
		"auto":   "SELECT CONCAT_WS(',', @@GLOBAL.read_only) v",
		"select": "SELECT CONCAT_WS(',', @@GLOBAL.read_only) v",
		"pfs":    "SELECT variable_name, variable_value from performance_schema.global_variables WHERE variable_name in (?)",
		"show":   "SHOW GLOBAL VARIABLES WHERE variable_name in (?)",
	}

	params := map[string][]interface{}{
		"auto":   []interface{}{},
		"select": []interface{}{},
		"pfs":    []interface{}{"read_only"},
		"show":   []interface{}{"read_only"},
	}

	for src, query := range queries {
		c := NewGlobal(db)
		plan := test.ReadPlan(t, "")
		plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_SOURCE] = src
		_, err := c.Prepare(context.Background(), plan)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t,
			query,
			c.queryIn["kpi"],
		)
		if diff := deep.Equal(params[src], c.paramsIn["kpi"]); diff != nil {
			t.Error(diff)
		}
	}
}

func TestPrepareWithCustomInvalidSource(t *testing.T) {
	// Given a LevelPlan for a collector with single level and a custom source,
	// which is invalid, test that Prepare returns an error
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewGlobal(db)
	plan := test.ReadPlan(t, "")
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_SOURCE] = "this_causes_error"
	_, err = c.Prepare(context.Background(), plan)
	assert.Error(t, err)
}

func TestPrepareWithInvalidMetricName(t *testing.T) {
	// Given a LevelPlan for a collector with single level and
	// invalid metricname, test that Prepare returns an error
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewGlobal(db)

	plan := test.ReadPlan(t, "")
	dom := plan.Levels["kpi"].Collect[DOMAIN]
	dom.Metrics = []string{"max_connections'); DROP TABLE students;--,", "max_prepared_stmt_count"}
	dom.Options[OPT_SOURCE] = "select"
	plan.Levels["kpi"].Collect[DOMAIN] = dom

	_, err = c.Prepare(context.Background(), plan)
	assert.Error(t, err)

	// The PFS and SHOW sources use interpolated queries and will not have a syntax error
	// due to a bad metric name as a result.
	dom.Options[OPT_SOURCE] = "pfs"
	_, err = c.Prepare(context.Background(), plan)
	assert.Nil(t, err)

	dom.Options[OPT_SOURCE] = "show"
	_, err = c.Prepare(context.Background(), plan)
	assert.Nil(t, err)
}

// Metrics collected in test/plans/var_global.yaml
var fourMetrics = []string{"max_connections", "max_prepared_stmt_count", "innodb_log_file_size", "innodb_max_dirty_pages_pct"}

func TestCollectWithSingleLevelPlanAndNoSource(t *testing.T) {
	// Given a plan with single level containing a list of metrics for domain
	// and no custom source, verify that those metrics are retrieved correctly.
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewGlobal(db)
	plan := test.ReadPlan(t, "../../test/plans/var_global.yaml")
	_, err = c.Prepare(context.Background(), plan)
	if err != nil {
		t.Error(err)
	}
	metrics, _ := c.Collect(context.TODO(), "kpi")
	metricKeys := make([]string, 0, len(metrics))
	for _, m := range metrics {
		metricKeys = append(metricKeys, m.Name)
	}
	assert.ElementsMatch(t, metricKeys, fourMetrics)
}

func TestCollectWithAllSources(t *testing.T) {
	// Given a level plan with custom source option
	// collect should return the list of metrics correctly.
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	// Test this for all source types ie; auto, select, pfs and show

	srcs := [4]string{"auto", "select", "pfs", "show"}

	for _, src := range srcs {
		c := NewGlobal(db)
		plan := test.ReadPlan(t, "../../test/plans/var_global.yaml")
		plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_SOURCE] = src

		_, err := c.Prepare(context.Background(), plan)
		if err != nil {
			t.Error(err)
		}
		metrics, _ := c.Collect(context.TODO(), "kpi")
		metricKeys := make([]string, 0, len(metrics))
		for _, m := range metrics {
			metricKeys = append(metricKeys, m.Name)
		}
		assert.ElementsMatch(t, metricKeys, fourMetrics)
	}
}

func TestCollectWithMultipleLevels(t *testing.T) {
	// Given a plan with multiple levels containing a list of metrics for each level
	// verify that those metrics are retrieved correctly only at their respective levels.
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewGlobal(db)
	plan := test.ReadPlan(t, "../../test/plans/var_global_2_levels.yaml")
	_, err = c.Prepare(context.Background(), plan)
	if err != nil {
		t.Error(err)
	}
	metrics, _ := c.Collect(context.TODO(), "level_1")
	metricKeys := make([]string, 0, len(metrics))
	for _, m := range metrics {
		metricKeys = append(metricKeys, m.Name)
	}
	assert.ElementsMatch(t, metricKeys, []string{"max_connections", "max_prepared_stmt_count"})

	metrics, _ = c.Collect(context.TODO(), "level_2")
	metricKeys = make([]string, 0, len(metrics))
	for _, m := range metrics {
		metricKeys = append(metricKeys, m.Name)
	}
	assert.ElementsMatch(t, metricKeys, []string{"innodb_log_file_size", "innodb_max_dirty_pages_pct"})
}

func TestCollectWithOneNonExistentMetric(t *testing.T) {
	// Given a level plan with single level which contains 3 valid metrics and
	// 1 non existent metric, it should successfully return 3 metrics
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewGlobal(db)
	plan := test.ReadPlan(t, "../../test/plans/var_global_bad_metric.yaml")
	_, err = c.Prepare(context.Background(), plan)
	if err != nil {
		t.Error(err)
	}
	metrics, _ := c.Collect(context.TODO(), "kpi")
	metricKeys := make([]string, 0, len(metrics))
	for _, m := range metrics {
		metricKeys = append(metricKeys, m.Name)
	}
	assert.ElementsMatch(t, metricKeys, []string{"max_connections", "max_prepared_stmt_count", "innodb_max_dirty_pages_pct"})
}
