// Copyright 2024 Block, Inc.

package monitor_test

import (
	"strings"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan/default"
	"github.com/cashapp/blip/v2/test"
)

func TestProm(t *testing.T) {
	// Test that the Prometheus-emulating Exporter scrapes from MySQL
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	exp := monitor.NewExporter(
		blip.ConfigExporter{},
		default_plan.Exporter(),
		monitor.NewEngine(blip.ConfigMonitor{MonitorId: "exp1"}, db),
	)

	got, err := exp.Scrape()
	if err != nil {
		t.Error(err)
	}

	// The real output is very long, but we can be certain the basic mechanics
	// are working if it continas this exact snippet, which means it must have
	// successfully queried MySQL and printed the output (the metrics in Exposition
	// format native to Prometheus, not Blip).
	expect := `# HELP mysql_global_variables_port Generic gauge metric.
# TYPE mysql_global_variables_port gauge
mysql_global_variables_port 3306`

	if !strings.Contains(got, expect) {
		t.Errorf("output does not contain:\n%s\n\n%s\n", expect, got)
	}
}
