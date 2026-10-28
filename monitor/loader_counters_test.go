// Copyright 2026 Block, Inc.

package monitor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/sink"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

// counterLoader returns a Loader that loads the monitor configs returned by
// the configs func, so a test can change configs between calls to Load.
func counterLoader(stopLoss string, configs func() []blip.ConfigMonitor) *Loader {
	cfg := blip.DefaultConfig()
	cfg.MonitorLoader.StopLoss = stopLoss
	return NewLoader(LoaderArgs{
		Config:     cfg,
		Factories:  blip.Factories{DbConn: dbconn.NewConnFactory(nil, nil)},
		PlanLoader: plan.NewLoader(nil),
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
		Plugins: blip.Plugins{
			LoadMonitors: func(blip.Config) ([]blip.ConfigMonitor, error) { return configs(), nil },
		},
	})
}

// chronosphereMonitor returns a monitor config with one chronosphere sink,
// which the sink factory wraps in Retry. The sink URL port is unreachable, so
// every send fails and stays in the retry buffer.
func chronosphereMonitor(monitorId, port string) blip.ConfigMonitor {
	return blip.ConfigMonitor{
		MonitorId: monitorId,
		Username:  "root",
		Password:  "test",
		Hostname:  "127.0.0.1:" + test.MySQLPort[test.DefaultMySQLVersion],
		Sinks: map[string]map[string]string{
			"chronosphere": {"url": "http://127.0.0.1:" + port + "/openmetrics/write"},
		},
	}
}

const chronosphereRetryDropped = status.SINK_RETRY_DROPPED + ":chronosphere"

// waitForCounter waits for the monitor to start and register the counter.
func waitForCounter(t *testing.T, monitorId, name string) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := status.ReportCounters(monitorId)[monitorId][name]; ok {
			return v
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: counter %s not reported after monitor start: %v", monitorId, name, status.ReportCounters(monitorId))
	return 0
}

func TestLoaderReloadKeepsSinkCounters(t *testing.T) {
	if _, _, err := test.Connection(test.DefaultMySQLVersion); err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}

	const monitorId = "reload-sink-counters"
	defer status.RemoveMonitor(monitorId)

	var mux sync.Mutex
	moncfg := chronosphereMonitor(monitorId, "1")
	ml := counterLoader("", func() []blip.ConfigMonitor {
		mux.Lock()
		defer mux.Unlock()
		return []blip.ConfigMonitor{moncfg}
	})
	defer ml.Unload(monitorId, true)

	if err := ml.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := status.ReportCounters(monitorId); len(got) != 0 {
		t.Errorf("counters reported before monitor start: %v", got)
	}
	ml.StartMonitors()
	waitForCounter(t, monitorId, chronosphereRetryDropped)
	old := ml.Monitor(monitorId)

	// Change the config, which makes a new monitor (and new sinks) with the
	// same ID, then unloads the old monitor. The new monitor's sink counter
	// must be reported once it starts.
	mux.Lock()
	moncfg = chronosphereMonitor(monitorId, "2")
	mux.Unlock()
	if err := ml.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	mon := ml.Monitor(monitorId)
	if mon == old {
		t.Fatal("config change did not make a new monitor")
	}
	ml.StartMonitors()
	before := waitForCounter(t, monitorId, chronosphereRetryDropped)

	// Overfill the retry buffer. With a canceled context, Send only buffers
	// the metrics; it doesn't wait to retry. The monitor might also be sending
	// (and dropping) its own metrics, so expect at least one more drop.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i <= sink.DEFAULT_RETRY_BUFFER_SIZE; i++ {
		mon.sinks[0].Send(ctx, &blip.Metrics{MonitorId: monitorId, Values: map[string][]blip.MetricValue{}})
	}
	if after := status.ReportCounters(monitorId)[monitorId][chronosphereRetryDropped]; after < before+1 {
		t.Errorf("%s = %d after overfilling the retry buffer, expected > %d", chronosphereRetryDropped, after, before)
	}
}

func TestLoaderUnloadedMonitorsHaveNoCounters(t *testing.T) {
	// Making a monitor makes its sinks, but neither registers counters: only
	// starting a monitor does. So a load that makes monitors but doesn't add
	// them leaves no counters for them.
	ids := []string{"stoploss-1", "stoploss-2", "stoploss-3", "stoploss-4"}
	const newId = "stoploss-new"
	defer func() {
		for _, id := range append(ids, newId) {
			status.RemoveMonitor(id)
		}
	}()

	var mux sync.Mutex
	configs := []blip.ConfigMonitor{}
	for _, id := range ids {
		configs = append(configs, chronosphereMonitor(id, "1"))
	}
	ml := counterLoader("1", func() []blip.ConfigMonitor {
		mux.Lock()
		defer mux.Unlock()
		return configs
	})
	if err := ml.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := status.ReportCounters(ids...); len(got) != 0 {
		t.Errorf("counters reported for loaded but not started monitors: %v", got)
	}

	// Remove 2 of 4 monitors (stop-loss 1) and add a new one: Load makes the
	// new monitor, then returns ErrStopLoss without adding it.
	mux.Lock()
	configs = []blip.ConfigMonitor{configs[0], configs[1], chronosphereMonitor(newId, "1")}
	mux.Unlock()
	if err := ml.Load(context.Background()); err != ErrStopLoss {
		t.Fatalf("Load returned %v, expected ErrStopLoss", err)
	}
	if n := ml.Count(); n != uint(len(ids)) {
		t.Fatalf("%d monitors loaded after ErrStopLoss, expected %d (unchanged)", n, len(ids))
	}
	if got := status.ReportCounters(newId); len(got) != 0 {
		t.Errorf("counters reported for monitor not loaded due to stop-loss: %v", got)
	}
}
