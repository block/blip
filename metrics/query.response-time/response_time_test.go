// Copyright 2024 Block, Inc.

package queryresponsetime

import (
	"context"
	"testing"

	"github.com/cashapp/blip/v2/sqlutil"
	"github.com/cashapp/blip/v2/test"
)

func TestCollectP(t *testing.T) {
	_, db, err := test.Connection("mysql80")
	if err != nil {
		t.Skip("mysql80 not running")
	}
	defer db.Close()

	c := NewResponseTime(db)

	// Plan collects p95 and P99. The second should be converted to lowercase p99.
	plan := test.ReadPlan(t, "../../test/plans/mysql_qrt.yaml")
	_, err = c.Prepare(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}

	metrics, err := c.Collect(context.Background(), "kpi")
	if err != nil {
		t.Error(err)
	}
	if len(metrics) != 2 {
		t.Fatalf("collected %d metrics, expected 2: %+v", len(metrics), metrics)
	}

	// The metric names should match what's listed in the plan (but lowercase)
	if metrics[0].Name != "p95" {
		t.Errorf("metrics[0].Name = %s, expected p95", metrics[0].Name)
	}
	if metrics[1].Name != "p99" { // lowercase
		t.Errorf("metrics[1].Name = %s, expected p99", metrics[1].Name)
	}

	// No way to know what the vaules will be, but we know that
	// p99 must be >= p95
	if metrics[1].Value < metrics[0].Value {
		t.Errorf("p99 = %f < p95 = %f", metrics[1].Value, metrics[0].Value)
	}

	// By default, meta include real P values: p95 =~ p95.2
	r, ok := metrics[0].Meta["p95"]
	if !ok {
		t.Errorf("metrics[0].Meta doesn't have key p95: %+v", metrics[0].Meta)
	}
	f, err := sqlutil.ParsePercentileStr(r)
	if err != nil {
		t.Error(err)
	}
	if f < 0.95 {
		t.Errorf("metrics[0] real %s %f < 0.95, expected >= 0.95", r, f)
	}

	r, ok = metrics[1].Meta["p99"]
	if !ok {
		t.Errorf("metrics[0].Meta doesn't have key p99: %+v", metrics[1].Meta)
	}
	f, err = sqlutil.ParsePercentileStr(r)
	if err != nil {
		t.Error(err)
	}
	if f < 0.99 {
		t.Errorf("metrics[1] real %s %f < 0.99, expected >= 0.99", r, f)
	}
}
