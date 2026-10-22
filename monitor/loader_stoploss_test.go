// Copyright 2026 Block, Inc.

package monitor_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestLoaderStopLoss(t *testing.T) {
	// Every case loads 10 monitors, then reloads after removing, adding, and
	// changing some. Stop-loss is the net drop in the number of monitors:
	// removed minus added. Changed monitors don't count. The drop must be
	// greater than the limit to trigger stop-loss.
	const nBefore = 10
	tests := []struct {
		name     string
		stopLoss string
		removed  int
		added    int
		changed  int
		stopped  bool // expect ErrStopLoss
	}{
		{name: "number below", stopLoss: "3", removed: 2},
		{name: "number at", stopLoss: "3", removed: 3},
		{name: "number above", stopLoss: "3", removed: 4, stopped: true},
		{name: "number all", stopLoss: "3", removed: 10, stopped: true},
		{name: "number below with new", stopLoss: "3", removed: 3, added: 1},
		{name: "number at with new", stopLoss: "3", removed: 4, added: 1},
		{name: "number above with new", stopLoss: "3", removed: 5, added: 1, stopped: true},
		{name: "number replaced", stopLoss: "3", removed: 5, added: 5},
		{name: "number changed", stopLoss: "3", changed: 5},
		{name: "percent below", stopLoss: "30%", removed: 2},
		{name: "percent at", stopLoss: "30%", removed: 3},
		{name: "percent above", stopLoss: "30%", removed: 4, stopped: true},
		{name: "percent all", stopLoss: "30%", removed: 10, stopped: true},
		{name: "percent below with new", stopLoss: "30%", removed: 3, added: 1},
		{name: "percent at with new", stopLoss: "30%", removed: 4, added: 1},
		{name: "percent above with new", stopLoss: "30%", removed: 5, added: 1, stopped: true},
		{name: "percent replaced", stopLoss: "30%", removed: 5, added: 5},
		{name: "percent changed", stopLoss: "30%", changed: 5},
		{name: "disabled", stopLoss: "", removed: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mux sync.Mutex
			configs := make([]blip.ConfigMonitor, nBefore)
			for i := range configs {
				configs[i] = stopLossMonitor(fmt.Sprintf("stoploss-%d", i))
			}
			ml := stopLossLoader(tt.stopLoss, func() []blip.ConfigMonitor {
				mux.Lock()
				defer mux.Unlock()
				return configs
			})
			require.NoError(t, ml.Load(context.Background()))
			require.Equal(t, uint(nBefore), ml.Count())
			defer func() {
				for _, mon := range ml.Monitors() {
					ml.Unload(mon.MonitorId(), true)
				}
			}()

			// Remove the first monitors, change the next ones, and add new ones.
			mux.Lock()
			next := append([]blip.ConfigMonitor{}, configs[tt.removed:]...)
			for i := 0; i < tt.changed; i++ {
				next[i].Password = "changed"
			}
			for i := 0; i < tt.added; i++ {
				next = append(next, stopLossMonitor(fmt.Sprintf("stoploss-new-%d", i)))
			}
			configs = next
			mux.Unlock()

			err := ml.Load(context.Background())
			if tt.stopped {
				assert.ErrorIs(t, err, monitor.ErrStopLoss)
				assert.Equal(t, uint(nBefore), ml.Count(), "monitors changed after ErrStopLoss")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, uint(nBefore-tt.removed+tt.added), ml.Count())
			}
		})
	}
}

// stopLossLoader returns a Loader with the given stop-loss that loads the
// monitor configs returned by the configs func. The monitors aren't started.
func stopLossLoader(stopLoss string, configs func() []blip.ConfigMonitor) *monitor.Loader {
	cfg := blip.DefaultConfig()
	cfg.MonitorLoader.StopLoss = stopLoss
	return monitor.NewLoader(monitor.LoaderArgs{
		Config:     cfg,
		Factories:  blip.Factories{DbConn: dbconn.NewConnFactory(nil, nil)},
		PlanLoader: plan.NewLoader(nil),
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
		Plugins: blip.Plugins{
			LoadMonitors: func(blip.Config) ([]blip.ConfigMonitor, error) { return configs(), nil },
		},
	})
}

func stopLossMonitor(monitorId string) blip.ConfigMonitor {
	return blip.ConfigMonitor{
		MonitorId: monitorId,
		Hostname:  "127.0.0.1:1",
		Sinks:     map[string]map[string]string{"noop": {}},
	}
}
