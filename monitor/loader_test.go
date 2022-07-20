// Copyright 2022 Block, Inc.

package monitor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/aws"
	"github.com/cashapp/blip/dbconn"
	"github.com/cashapp/blip/monitor"
	"github.com/cashapp/blip/plan"
	"github.com/cashapp/blip/test/mock"
)

func monitorIds(monitors []*monitor.Monitor) []string {
	ids := make([]string, len(monitors))
	for i := range monitors {
		ids[i] = monitors[i].MonitorId()
	}
	return ids
}

// --------------------------------------------------------------------------

func TestLoaderLoadOne(t *testing.T) {
	// Test most basic monitor loader function: loading one monitor from the
	// a Bilp config. The config details don't really matter here; we just
	// want to see that the Loader loads the monitor, which we detect using
	// its optiona RunMonitor callback to both inspect what it loaded and
	// prevent it from actually running the monitor.
	planName := "../test/plans/lpc_1_5_10.yaml"
	moncfg := blip.ConfigMonitor{
		MonitorId: monitorId1,
		Username:  "root",
		Password:  "test",
		Hostname:  "127.0.0.1:33560", // 5.6
	}
	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{planName}},
		Monitors: []blip.ConfigMonitor{moncfg},
	}

	// Create a new Loader and call its main method: Load.
	args := monitor.LoaderArgs{
		Config: cfg,
		Factories: blip.Factories{
			DbConn: dbconn.NewConnFactory(nil, nil),
		},
		Plugins: blip.Plugins{TransformMetrics: func(metrics *blip.Metrics) error {
			return nil
		}},
		PlanLoader: plan.NewLoader(nil),
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
	}
	loader := monitor.NewLoader(args)
	err := loader.Load(context.Background())
	if err != nil {
		t.Error(err)
	}

	// Verify that it loaded the one monitor
	gotIds := monitorIds(loader.Monitors())
	expectIds := []string{moncfg.MonitorId}
	assert.ElementsMatch(t, gotIds, expectIds)
}
