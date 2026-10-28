// Copyright 2026 Block, Inc.

package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test/mock"
)

// runStopLCO runs an LCO with one collector, calls ready to wait until it's
// running, then stops it. It returns how long Run took to return after stop.
func runStopLCO(t *testing.T, c *lco, ready func()) time.Duration {
	t.Helper()
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go c.Run(stopChan, doneChan)
	ready()
	t0 := time.Now()
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Minute):
		t.Fatal("LCO did not stop")
	}
	return time.Since(t0)
}

func newStopTestLCO(monitorId, level string, collector blip.Collector) *lco {
	c := newLevelCollectorWithDBProvider(LevelCollectorArgs{Config: blip.ConfigMonitor{MonitorId: monitorId}}, nil)
	c.engine = faultTestEngine(monitorId, level, collector)
	c.plan = c.engine.plan
	c.levels = []plan.SortedLevel{{Freq: time.Second, Name: level}}
	c.emr = time.Second
	c.paused = false
	return c
}

func TestLevelCollectorStopDoesNotWaitForTick(t *testing.T) {
	// Run waits for the next tick or stop, so a stop doesn't wait for the
	// tick. Use a long tick: before, a stop right after Run started waited
	// for the whole tick.
	const monitorId = "lco-stop-tick"
	defer status.RemoveMonitor(monitorId)
	const tick = 10 * time.Second
	TickerDuration(tick, time.Second)
	defer TickerDuration(time.Second, time.Second)

	c := newStopTestLCO(monitorId, "every", mock.MetricsCollector{DomainFunc: func() string { return "test.lco-stop-tick" }})
	d := runStopLCO(t, c, func() {})
	if d > tick/10 {
		t.Errorf("stop took %s, expected well under the %s tick", d, tick)
	}
}

func TestLevelCollectorStopCancelsCollection(t *testing.T) {
	// A stop cancels a collection in progress instead of waiting for it to
	// finish, which can take up to the engine max runtime (EMR). The canceled
	// collection isn't counted as an EMR timeout.
	const monitorId = "lco-stop-collect"
	defer status.RemoveMonitor(monitorId)
	TickerDuration(10*time.Millisecond, time.Second)
	defer TickerDuration(time.Second, time.Second)

	started := make(chan struct{}, 1)
	slow := mock.MetricsCollector{
		DomainFunc: func() string { return "test.lco-stop-collect" },
		CollectFunc: func(ctx context.Context, _ string) ([]blip.MetricValue, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done() // until canceled or the collector max runtime
			return nil, ctx.Err()
		},
	}
	const maxRuntime = time.Minute
	c := newStopTestLCO(monitorId, "every", slow)
	c.emr = maxRuntime
	for _, cl := range c.engine.collectors {
		cl.cmr = maxRuntime
	}

	d := runStopLCO(t, c, func() {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for collection to start")
		}
	})
	if d > maxRuntime/10 {
		t.Errorf("stop took %s, expected well under the %s max runtime", d, maxRuntime)
	}
	if v := counters(t, monitorId)[status.ENGINE_EMR_TIMEOUTS]; v != 0 {
		t.Errorf("%s = %d, expected 0", status.ENGINE_EMR_TIMEOUTS, v)
	}
	// The canceled collection's metrics are dropped, not reported collected
	if s := status.ReportMonitors(monitorId)[monitorId][status.LEVEL_COLLECT]; !strings.HasSuffix(s, ": stopped; metrics dropped") {
		t.Errorf("%s status = %q, expected stopped; metrics dropped", status.LEVEL_COLLECT, s)
	}
}

func TestLevelCollectorStopDropsQueuedMetrics(t *testing.T) {
	// Metrics from a complete collection that haven't been sent to the sinks
	// when the LCO stops are dropped, not sent after the monitor stopped.
	// Block TransformMetrics so that metrics are received and queued, then
	// stop the LCO, then let TransformMetrics return.
	const monitorId = "lco-stop-queued"
	defer status.RemoveMonitor(monitorId)
	TickerDuration(10*time.Millisecond, time.Second)
	defer TickerDuration(time.Second, time.Second)

	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var sent atomic.Int32
	c := newStopTestLCO(monitorId, "every", mock.MetricsCollector{DomainFunc: func() string { return "test.lco-stop-queued" }})
	c.transformMetrics = func([]*blip.Metrics) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	}
	c.sinks = []blip.Sink{mock.Sink{SendFunc: func(context.Context, *blip.Metrics) error {
		sent.Add(1)
		return nil
	}}}

	runStopLCO(t, c, func() {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for TransformMetrics")
		}
		time.Sleep(50 * time.Millisecond) // queue more metrics
	})
	releaseOnce.Do(func() { close(release) })
	time.Sleep(100 * time.Millisecond)
	if n := sent.Load(); n != 0 {
		t.Errorf("sinks got %d sends after stop, expected 0", n)
	}
	// The sinks status isn't left at "TransformMetrics"
	if s, ok := status.ReportMonitors(monitorId)[monitorId][status.LEVEL_SINKS]; ok {
		t.Errorf("%s status = %q, expected none after the metrics were dropped", status.LEVEL_SINKS, s)
	}
}

func TestLevelCollectorStopDropsMetricsAfterSend(t *testing.T) {
	// After a sink Send returns, recvMetrics can receive metrics that were
	// queued during the Send even though the LCO has stopped, because select
	// picks randomly when both are ready. It must drop them before it calls
	// TransformMetrics. Block the first Send so that metrics queue, stop the
	// LCO, then let the Send return. One run catches a missing check half the
	// time, so run several.
	const monitorId = "lco-stop-after-send"
	defer status.RemoveMonitor(monitorId)
	TickerDuration(10*time.Millisecond, time.Second)
	defer TickerDuration(time.Second, time.Second)

	for i := 0; i < 20; i++ {
		entered := make(chan struct{})
		release := make(chan struct{})
		var enteredOnce, releaseOnce sync.Once
		var transforms atomic.Int32
		c := newStopTestLCO(monitorId, "every", mock.MetricsCollector{DomainFunc: func() string { return "test.lco-stop-after-send" }})
		c.transformMetrics = func([]*blip.Metrics) error {
			transforms.Add(1)
			return nil
		}
		c.sinks = []blip.Sink{mock.Sink{SendFunc: func(context.Context, *blip.Metrics) error {
			enteredOnce.Do(func() {
				close(entered)
				<-release
			})
			return nil
		}}}

		runStopLCO(t, c, func() {
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for sink Send")
			}
			time.Sleep(30 * time.Millisecond) // queue more metrics
		})
		before := transforms.Load()
		releaseOnce.Do(func() { close(release) })
		time.Sleep(30 * time.Millisecond)
		if n := transforms.Load() - before; n != 0 {
			t.Fatalf("run %d: TransformMetrics called %d times after stop, expected 0", i, n)
		}
	}
}

func TestLevelCollectorStopDuringSendDoesNotAddStatus(t *testing.T) {
	// A sink Send in progress when the LCO stops can return after the Loader
	// has removed the monitor's status. recvMetrics must not set a status
	// then, which would add back the status of a removed monitor. Block the
	// first Send, stop the LCO, remove the status, then let the Send return.
	for _, sendErr := range []error{errors.New("send failed"), nil} {
		t.Run(fmt.Sprintf("err=%v", sendErr), func(t *testing.T) {
			const monitorId = "lco-stop-during-send"
			defer status.RemoveMonitor(monitorId)
			TickerDuration(10*time.Millisecond, time.Second)
			defer TickerDuration(time.Second, time.Second)

			entered := make(chan struct{})
			release := make(chan struct{})
			returned := make(chan struct{})
			var once sync.Once
			c := newStopTestLCO(monitorId, "every", mock.MetricsCollector{DomainFunc: func() string { return "test.lco-stop-during-send" }})
			c.sinks = []blip.Sink{mock.Sink{SendFunc: func(context.Context, *blip.Metrics) error {
				err := errors.New("not first send")
				once.Do(func() {
					close(entered)
					<-release
					defer close(returned)
					err = sendErr
				})
				return err
			}}}

			runStopLCO(t, c, func() {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("timeout waiting for sink Send")
				}
			})
			status.RemoveMonitor(monitorId)
			close(release)
			<-returned
			time.Sleep(50 * time.Millisecond) // let recvMetrics return
			if s, ok := status.ReportMonitors(monitorId)[monitorId]; ok {
				t.Errorf("status of removed monitor added back: %v", s)
			}
		})
	}
}

// cancelAfterFirstCheck is a context that reports canceled after the first
// Err call. In collect, that's the check right after Engine.Collect, so it's
// as if the LCO stopped after collect queued the metrics.
type cancelAfterFirstCheck struct {
	context.Context
	checks atomic.Int32
}

func (c *cancelAfterFirstCheck) Err() error {
	if c.checks.Add(1) > 1 {
		return context.Canceled
	}
	return nil
}

func TestLevelCollectorStopAfterQueueReportsCollected(t *testing.T) {
	// If the LCO stops after collect queued the metrics, collect reports
	// them collected, not dropped: recvMetrics drops them, if it hasn't sent
	// them yet, and clears the sinks status.
	const monitorId = "lco-stop-after-queue"
	defer status.RemoveMonitor(monitorId)
	c := newStopTestLCO(monitorId, "every", mock.MetricsCollector{DomainFunc: func() string { return "test.lco-stop-after-queue" }})
	c.collect(&cancelAfterFirstCheck{Context: context.Background()}, 1, "every", time.Now())
	if n := len(c.metricsChan); n != 1 {
		t.Fatalf("%d metrics queued, expected 1", n)
	}
	if s := status.ReportMonitors(monitorId)[monitorId][status.LEVEL_COLLECT]; !strings.Contains(s, ": collected in ") {
		t.Errorf("%s status = %q, expected collected in", status.LEVEL_COLLECT, s)
	}
}
