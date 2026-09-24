// Copyright 2026 Block, Inc.

package stmtschema

import (
	"context"
	"strings"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test"
)

func TestSummaryQuery(t *testing.T) {
	query, params, names, err := SummaryQuery(map[string]string{
		OPT_INCLUDE: "app, other",
		OPT_EXCLUDE: "ignored",
	}, []string{"COUNT_STAR", "sum_errors", "count_star"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "SELECT SCHEMA_NAME, SUM(count_star) AS count_star, SUM(sum_errors) AS sum_errors FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME IS NOT NULL AND DIGEST IS NOT NULL AND SCHEMA_NAME IN (?, ?) GROUP BY SCHEMA_NAME"; query != want {
		t.Errorf("query:\n%s\nwant:\n%s", query, want)
	}
	if len(names) != 2 || names[0] != "count_star" || names[1] != "sum_errors" {
		t.Errorf("names = %v", names)
	}
	if len(params) != 2 || params[0] != "app" || params[1] != "other" {
		t.Errorf("params = %v", params)
	}

	query, _, names, err = SummaryQuery(nil, nil)
	if err != nil || len(names) == 0 || !strings.Contains(query, "SCHEMA_NAME NOT IN (?, ?, ?, ?)") {
		t.Errorf("default query = %q, names = %v, err = %v", query, names, err)
	}
}

func TestCollectMySQL80(t *testing.T) {
	_, db, err := test.Connection("mysql80")
	if err != nil {
		t.Skip("mysql80 not running")
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "USE mysql"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT 42"); err != nil {
		t.Fatal(err)
	}

	c := NewSchema(db)
	plan := blip.Plan{Levels: map[string]blip.Level{
		"test": {Name: "test", Collect: map[string]blip.Domain{
			DOMAIN: {Metrics: []string{"count_star", "sum_rows_sent"}, Options: map[string]string{OPT_INCLUDE: "mysql"}},
		}},
	}}
	if _, err := c.Prepare(ctx, plan); err != nil {
		t.Fatal(err)
	}
	values, err := c.Collect(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0].Group["db"] != "mysql" || values[0].Value < 1 {
		t.Fatalf("unexpected schema metrics: %+v", values)
	}
}

func TestSummaryQueryRejectsUnknownMetric(t *testing.T) {
	_, _, _, err := SummaryQuery(nil, []string{"avg_timer_wait"})
	if err == nil {
		t.Fatal("expected unknown metric error")
	}
}
