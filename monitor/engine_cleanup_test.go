// Copyright 2026 Block, Inc.

package monitor

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/metrics"
	repllag "github.com/cashapp/blip/v2/metrics/repl.lag"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestEnginePrepareStopDoesNotLeakHeartbeatReader(t *testing.T) {
	// Engine.Prepare only pings the database, and the repl.lag blip writer
	// doesn't query it, so MySQL isn't needed
	db := mock.PingDB()
	t.Cleanup(func() { db.Close() }) // after the engines stop: cleanups run last-in, first-out

	// repl.lag at two levels, like a plan where the slow level inherits
	// domains from the fast level
	lag := blip.Domain{
		Name:    repllag.DOMAIN,
		Options: map[string]string{repllag.OPT_WRITER: repllag.LAG_WRITER_BLIP},
	}
	plan := blip.Plan{
		Name:      "heartbeat-leak",
		MonitorId: "heartbeat-leak",
		Levels: map[string]blip.Level{
			"fast": {Name: "fast", Freq: "5s", Collect: map[string]blip.Domain{repllag.DOMAIN: lag}},
			"slow": {Name: "slow", Freq: "30s", Collect: map[string]blip.Domain{repllag.DOMAIN: lag}},
		},
	}

	// Count only readers started by this test
	readers := test.NewGoroutines()
	readers.Label()
	for i := 0; i < 3; i++ {
		engine := NewEngine(blip.ConfigMonitor{MonitorId: plan.MonitorId}, db)
		t.Cleanup(engine.Stop) // if the test fails before Stop; a second Stop is a no-op
		if err := engine.Prepare(context.Background(), plan, func() {}, func() {}); err != nil {
			t.Fatal(err)
		}
		if got := readers.Wait(test.HeartbeatReaderFunc, 1, test.HeartbeatReaderTimeout); got != 1 {
			t.Fatalf("cycle %d: %d heartbeat readers running after Prepare, expected 1", i, got)
		}
		engine.Stop()
		if got := readers.Wait(test.HeartbeatReaderFunc, 0, test.HeartbeatReaderTimeout); got != 0 {
			t.Fatalf("cycle %d: %d heartbeat readers running after Stop, expected 0", i, got)
		}
	}
}

func TestEnginePrepareErrorCleansUpPreparedCollectors(t *testing.T) {
	// Engine.Prepare only pings the database, and the mock collectors don't
	// query it, so MySQL isn't needed
	db := mock.PingDB()
	defer db.Close()

	// Two collectors. Domains are prepared in random order, so whichever is
	// prepared second fails. Prepare discards both collectors, so it must call
	// the cleanup func of the first. The second returns a cleanup func with its
	// error, which Prepare calls as a backstop (see blip.Collector.Prepare).
	domains := []string{"test.cleanup-a", "test.cleanup-b"}
	mux := &sync.Mutex{}
	prepared := 0
	cleanedUp := map[string]bool{}
	for _, domain := range domains {
		domain := domain
		collector := mock.MetricsCollector{
			DomainFunc: func() string { return domain },
			PrepareFunc: func(context.Context, blip.Plan) (func(), error) {
				mux.Lock()
				defer mux.Unlock()
				prepared++
				cleanup := func() {
					mux.Lock()
					cleanedUp[domain] = true
					mux.Unlock()
				}
				if prepared > 1 {
					return cleanup, errors.New("prepare failed")
				}
				return cleanup, nil
			},
		}
		if err := metrics.Register(domain, mock.MetricFactory{
			MakeFunc: func(string, blip.CollectorFactoryArgs) (blip.Collector, error) {
				return collector, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { metrics.Remove(domain) })
	}

	plan := blip.Plan{
		Name: "prepare-error",
		Levels: map[string]blip.Level{
			"fast": {
				Name:    "fast",
				Freq:    "1s",
				Collect: map[string]blip.Domain{domains[0]: {}, domains[1]: {}},
			},
		},
	}
	engine := NewEngine(blip.ConfigMonitor{MonitorId: "prepare-error"}, db)
	if err := engine.Prepare(context.Background(), plan, func() {}, func() {}); err == nil {
		t.Fatal("Prepare returned nil error, expected collector prepare error")
	}

	mux.Lock()
	defer mux.Unlock()
	for _, domain := range domains {
		if !cleanedUp[domain] {
			t.Errorf("%s cleanup not called after Prepare error", domain)
		}
	}
}
