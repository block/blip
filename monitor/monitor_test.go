// Copyright 2024 Block, Inc.

package monitor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestMonitor(t *testing.T) {
	_, _, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip("mysql57 not running")
		} else {
			t.Fatal(err)
		}
	}

	moncfg := blip.ConfigMonitor{
		MonitorId: "tm1",
		Username:  "root",
		Password:  "test",
		Hostname:  "127.0.0.1:" + test.MySQLPort[test.DefaultMySQLVersion],
	}
	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{"../test/plans/var_global.yaml"}},
		Monitors: []blip.ConfigMonitor{moncfg},
	}
	moncfg.ApplyDefaults(cfg)

	dbMaker := dbconn.NewConnFactory(nil, nil)
	pl := plan.NewLoader(nil)
	if err := pl.LoadShared(cfg.Plans, dbMaker); err != nil {
		t.Fatal(err)
	}
	if err := pl.LoadMonitor(moncfg, dbMaker); err != nil {
		t.Fatal(err)
	}

	metricsChan := make(chan *blip.Metrics, 1)
	sink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			metricsChan <- m
			return nil
		},
	}

	mon := monitor.NewMonitor(monitor.MonitorArgs{
		Config:     moncfg,
		PlanLoader: pl,
		DbMaker:    dbMaker,
		Sinks:      []blip.Sink{sink},
	})

	if err := mon.Start(); err != nil {
		t.Fatal(err)
	}

	var gotMetrics *blip.Metrics
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting to receive metrics")
	case gotMetrics = <-metricsChan:
	}

	if _, ok := gotMetrics.Values["var.global"]; !ok {
		t.Fatalf("did not collect var.global domain: %+v", gotMetrics.Values)
	}
	if len(gotMetrics.Values) != 1 {
		t.Errorf("collected %d domains, expected 1: %+v", len(gotMetrics.Values), gotMetrics.Values)
	}
	expectMetricValues := []blip.MetricValue{
		{
			Name:  "max_connections",
			Value: 151,
			Type:  blip.GAUGE,
		},
		{
			Name:  "max_prepared_stmt_count",
			Value: 16382,
			Type:  blip.GAUGE,
		},
		{
			Name:  "innodb_log_file_size",
			Value: 50331648,
			Type:  blip.GAUGE,
		},
		{
			Name:  "innodb_max_dirty_pages_pct",
			Value: 90,
			Type:  blip.GAUGE,
		},
	}
	assert.Equal(t, expectMetricValues, gotMetrics.Values["var.global"])

	err = mon.Stop()
	if err != nil {
		t.Error(err)
	}
}
