// Copyright 2024 Block, Inc.

package blip_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/plan/default"
	"github.com/cashapp/blip/v2/test"
)

// --------------------------------------------------------------------------

func TestPlanInterpolation(t *testing.T) {
	// Verify that env vars and monitor fields interpolate into plan options.
	// See this test plan for the particular env and monitor vars at each
	// level and domain:
	plan := test.ReadPlan(t, "./test/plans/interpolate1.yaml")

	mon := &blip.ConfigMonitor{
		MonitorId: "mon1",
		Hostname:  "db.local",
		Tags: map[string]string{
			"foo": "tag-foo",
			"bar": "tag-bar",
		},
		Meta: map[string]string{
			"foo": "meta-foo",
			"bar": "meta-bar",
		},
	}

	// Testing these two funcs:
	plan.InterpolateEnvVars()
	plan.InterpolateMonitor(mon)

	got := plan.Levels["level1"].Collect["domain1"].Options["opt1"]
	if got != "meta-foo" {
		t.Errorf("Got '%s', expected 'meta-foo' at level1.domain1.opt1", got)
	}

	got = plan.Levels["level1"].Collect["domain2"].Options["opt2"]
	expect := os.Getenv("TERM")
	if got != expect {
		t.Errorf("Got '%s', expected '%s' at level2.domain2.opt2", got, expect)
	}

	got = plan.Levels["level2"].Collect["domain1"].Options["opt3"]
	if got != "meta-bar" {
		t.Errorf("Got '%s', expected 'meta-bar' at level2.domain1.opt3", got)
	}

	got = plan.Levels["level2"].Collect["domain2"].Options["opt4"]
	expect = os.Getenv("SHELL")
	if got != expect {
		t.Errorf("Got '%s', expected '%s' at level2.domain2.opt2", got, expect)
	}
}

func TestValidateMetricName(t *testing.T) {
	// Test plan.Validate catches invalid metric names.

	// First, a completely VALID plan should not return an error
	plan := test.ReadPlan(t, "./test/plans/interpolate1.yaml")
	err := plan.Validate()
	if err != nil {
		t.Error(err)
	}

	// Now a plan with an invalid metric name: "foo bar"
	file := "./test/plans/invalid_metric_name.yaml"
	plan = test.ReadPlan(t, file)
	err = plan.Validate()
	if err == nil {
		t.Errorf("%s: Validate no error, expected error for invalid metric name", file)
	}
	// The error message should name the specific invalid metric name
	if !strings.Contains(err.Error(), "foo bar") {
		t.Errorf("%s: error message does not state invalid metric name, expected 'foo bar': %s",
			file, err)
	}
}

func TestValidateDefaultPlans(t *testing.T) {
	// Test that the default internal plans are valid because it'd be embarrassing
	// to ship invalid default plan
	plan := default_plan.MySQL()
	err := plan.Validate()
	if err != nil {
		t.Error(err)
	}

	plan = default_plan.Exporter()
	err = plan.Validate()
	if err != nil {
		t.Error(err)
	}
}

func TestPlanFreq(t *testing.T) {
	plan := default_plan.MySQL()
	min, domain := plan.Freq()

	d5s := time.Duration(5) * time.Second
	d300s := time.Duration(300) * time.Second
	d900s := time.Duration(900) * time.Second

	if min != d5s {
		t.Errorf("got min freq %d, expected 5", min)
	}

	expect := map[string]time.Duration{
		"status.global": d5s, // in two levels: 5s and 20s
		"innodb":        d5s,
		"repl":          d5s,
		"repl.lag":      d5s,
		"size.database": d300s, // 5min
		"size.table":    d300s, // 5min
		"size.binlog":   d300s, // 5min
		"var.global":    d900s, // 15min
	}
	if diff := deep.Equal(domain, expect); diff != nil {
		t.Error(diff)
	}
}
