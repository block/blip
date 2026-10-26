// Copyright 2024 Block, Inc.

package monitor_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/sink"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test/mock"
)

// These tests run real monitors (runLoop, LCO, engine) without a database:
// a fake SQL driver answers the engine's ping, and a fake collector controls
// how long a monitor takes to stop. Engine.Stop runs the collector cleanup
// func, and Monitor.Stop waits for it, so a cleanup that blocks makes the
// monitor block on stop.

const (
	stopTestDriver = "blip-loader-stop-test"
	stopTestDomain = "test.loaderstop"
)

func init() {
	sql.Register(stopTestDriver, stopTestSQLDriver{})
}

type stopTestSQLDriver struct{}

func (stopTestSQLDriver) Open(string) (driver.Conn, error) { return stopTestConn{}, nil }

type stopTestConn struct{}

func (stopTestConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no queries") }
func (stopTestConn) Close() error                        { return nil }
func (stopTestConn) Begin() (driver.Tx, error)           { return nil, errors.New("no transactions") }
func (stopTestConn) Ping(context.Context) error          { return nil }

type stopTestDbFactory struct{}

func (stopTestDbFactory) Make(cfg blip.ConfigMonitor) (*sql.DB, string, error) {
	db, err := sql.Open(stopTestDriver, cfg.MonitorId)
	return db, "fake/" + cfg.MonitorId, err
}

// stopTest controls the fake collector of every monitor in a test.
type stopTest struct {
	mux      sync.Mutex
	prepared map[string]int // monitor ID => number of successful Prepare calls
	cleanup  func(monitorId string)
	events   []event.Event
	sinks    map[string]map[string]string // monitor sinks; default noop
}

func (st *stopTest) preparedCount(monitorId string) int {
	st.mux.Lock()
	defer st.mux.Unlock()
	return st.prepared[monitorId]
}

func (st *stopTest) lastEventMsg(name, monitorId string) string {
	st.mux.Lock()
	defer st.mux.Unlock()
	msg := ""
	for _, e := range st.events {
		if e.Event == name && e.MonitorId == monitorId {
			msg = e.Message
		}
	}
	return msg
}

func (st *stopTest) eventCount(name, monitorId string) int {
	st.mux.Lock()
	defer st.mux.Unlock()
	n := 0
	for _, e := range st.events {
		if e.Event == name && (monitorId == "" || e.MonitorId == monitorId) {
			n++
		}
	}
	return n
}

// setupStopTest registers the fake collector and returns a Loader whose
// LoadMonitors plugin returns the monitor IDs in *ids (guarded by st.mux).
func setupStopTest(t *testing.T, stopTimeout string, ids *[]string, startMonitor func(blip.ConfigMonitor) bool) (*stopTest, *monitor.Loader) {
	t.Helper()
	st := &stopTest{
		prepared: map[string]int{},
		cleanup:  func(string) {},
	}

	mf := mock.MetricFactory{
		MakeFunc: func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
			monitorId := args.Config.MonitorId
			return mock.MetricsCollector{
				DomainFunc: func() string { return stopTestDomain },
				PrepareFunc: func(ctx context.Context, p blip.Plan) (func(), error) {
					st.mux.Lock()
					st.prepared[monitorId]++
					cleanup := st.cleanup
					st.mux.Unlock()
					return func() { cleanup(monitorId) }, nil
				},
			}, nil
		},
	}
	require.NoError(t, metrics.Register(stopTestDomain, mf))
	t.Cleanup(func() { metrics.Remove(stopTestDomain) })

	// The LCO checks for stop once per tick (1s), so a normal stop takes up to
	// 1s. Tick every 10ms so that only the fake collector's cleanup sets how
	// long a monitor takes to stop.
	monitor.TickerDuration(10*time.Millisecond, time.Second)
	t.Cleanup(func() { monitor.TickerDuration(time.Second, time.Second) })

	event.Subscribe(mock.EventReceiver{
		RecvFunc: func(e event.Event) {
			st.mux.Lock()
			st.events = append(st.events, e)
			st.mux.Unlock()
		},
	})
	t.Cleanup(event.RemoveSubscribers)

	testPlan := blip.Plan{
		Name: "loader-stop-test",
		Levels: map[string]blip.Level{
			"l1": {
				Name: "l1",
				Freq: "1s",
				Collect: map[string]blip.Domain{
					stopTestDomain: {Name: stopTestDomain},
				},
			},
		},
	}
	pl := plan.NewLoader(func(blip.ConfigPlans) ([]blip.Plan, error) {
		return []blip.Plan{testPlan}, nil
	})
	require.NoError(t, pl.LoadShared(blip.ConfigPlans{}, stopTestDbFactory{}))

	cfg := blip.DefaultConfig()
	cfg.MonitorLoader.StopTimeout = stopTimeout
	loader := monitor.NewLoader(monitor.LoaderArgs{
		Config:     cfg,
		Factories:  blip.Factories{DbConn: stopTestDbFactory{}},
		PlanLoader: pl,
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
		Plugins: blip.Plugins{
			LoadMonitors: func(blip.Config) ([]blip.ConfigMonitor, error) {
				st.mux.Lock()
				defer st.mux.Unlock()
				sinks := st.sinks
				if sinks == nil {
					sinks = map[string]map[string]string{"noop": {}}
				}
				mons := make([]blip.ConfigMonitor, len(*ids))
				for i, id := range *ids {
					mons[i] = blip.ConfigMonitor{
						MonitorId: id,
						Hostname:  "127.0.0.1:1",
						Sinks:     sinks,
					}
				}
				return mons, nil
			},
			StartMonitor: startMonitor,
		},
	})
	return st, loader
}

func setIds(st *stopTest, ids *[]string, newIds ...string) {
	st.mux.Lock()
	*ids = newIds
	st.mux.Unlock()
}

func makeIds(prefix string, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return ids
}

// waitPrepared waits until every monitor has prepared its plan (at least n times),
// which means its collector cleanup will run on stop.
func waitPrepared(t *testing.T, st *stopTest, n int, ids ...string) {
	t.Helper()
	for _, id := range ids {
		require.Eventually(t, func() bool { return st.preparedCount(id) >= n },
			5*time.Second, 5*time.Millisecond, "monitor %s not prepared %d times", id, n)
	}
}

// stopTestRetrySink is a Retry-wrapped sink that always fails to send. Its
// retry buffer holds 1 batch, so every send after the first drops a batch.
const stopTestRetrySink = "loader-stop-test-retry"

type stopTestRetrySinkFactory struct{}

func (stopTestRetrySinkFactory) Make(args blip.SinkFactoryArgs) (blip.Sink, error) {
	return sink.NewRetry(sink.RetryArgs{
		MonitorId: args.MonitorId,
		Sink: mock.Sink{SendFunc: func(context.Context, *blip.Metrics) error {
			return errors.New("send failed")
		}},
		BufferSize:    1,
		SendTimeout:   time.Millisecond,
		SendRetryWait: time.Millisecond,
	}), nil
}

var registerStopTestRetrySink sync.Once

func stopStatus(monitorId string) string {
	return status.ReportMonitors(monitorId)[monitorId]["error:stop"]
}

// --------------------------------------------------------------------------

func TestLoaderUnloadStuckMonitor(t *testing.T) {
	// A monitor that won't stop must not block Load or the Loader mutex. Load
	// returns after the stop timeout and reports the monitor. Re-adding the
	// same monitor ID must wait for the old monitor to stop, so the two never
	// run at the same time.
	ids := []string{"stuck1", "ok1"}
	st, loader := setupStopTest(t, "200ms", &ids, nil)
	defer status.RemoveMonitor("stuck1")
	defer status.RemoveMonitor("ok1")

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	st.cleanup = func(monitorId string) {
		if monitorId == "stuck1" {
			<-release
		}
	}

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, "stuck1", "ok1")

	// Remove both monitors. ok1 stops; stuck1 blocks in its cleanup.
	setIds(st, &ids)
	t0 := time.Now()
	require.NoError(t, loader.Load(context.Background()))
	d := time.Since(t0)
	assert.GreaterOrEqual(t, d, 200*time.Millisecond, "Load returned before the stop timeout")
	assert.Less(t, d, 2*time.Second, "Load blocked on the stuck monitor")
	assert.Empty(t, loader.Monitors())

	// The stuck monitor is reported, and the Loader isn't blocked
	assert.Eventually(t, func() bool { return st.eventCount(event.MONITOR_STOP_TIMEOUT, "stuck1") == 1 },
		time.Second, 5*time.Millisecond)
	assert.Equal(t, 0, st.eventCount(event.MONITOR_STOP_TIMEOUT, "ok1"))
	assert.Contains(t, stopStatus("stuck1"), "not stopped after 200ms")
	countDone := make(chan uint)
	go func() { countDone <- loader.Count() }()
	select {
	case n := <-countDone:
		assert.Equal(t, uint(0), n)
	case <-time.After(time.Second):
		t.Fatal("Loader mutex blocked by stuck monitor")
	}

	// Re-add stuck1 while its old monitor is still stopping. The new monitor
	// is loaded but must not start until the old one has stopped.
	setIds(st, &ids, "stuck1")
	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	require.NoError(t, loader.Start("stuck1", true)) // pending start is not an error
	assert.Equal(t, []string{"stuck1"}, monitorIds(loader.Monitors()))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, st.preparedCount("stuck1"), "new monitor started while old monitor was still stopping")
	assert.Contains(t, status.ReportMonitors("stuck1")["stuck1"][status.MONITOR], "start pending")

	// Let the old monitor finish stopping: the new monitor starts, and the
	// stuck report is cleared
	releaseOnce.Do(func() { close(release) })
	waitPrepared(t, st, 2, "stuck1")
	assert.Empty(t, stopStatus("stuck1"))

	// The new monitor stops normally
	setIds(st, &ids)
	t0 = time.Now()
	require.NoError(t, loader.Load(context.Background()))
	assert.Less(t, time.Since(t0), 200*time.Millisecond)
	assert.Equal(t, 1, st.eventCount(event.MONITOR_STOP_TIMEOUT, "stuck1"))
}

func TestLoaderStopStuckMonitor(t *testing.T) {
	// Loader.Stop (used by POST /monitors/stop) waits no longer than the stop
	// timeout, and Start while the monitor is still stopping is deferred
	// until the stop finishes.
	ids := []string{"stuck2"}
	st, loader := setupStopTest(t, "100ms", &ids, nil)
	defer status.RemoveMonitor("stuck2")

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	st.cleanup = func(string) { <-release }

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, "stuck2")

	t0 := time.Now()
	err := loader.Stop("stuck2", true)
	assert.ErrorIs(t, err, monitor.ErrStopTimeout)
	assert.Less(t, time.Since(t0), time.Second)

	// A second stop of the same monitor waits on the same stop
	assert.ErrorIs(t, loader.Stop("stuck2", true), monitor.ErrStopTimeout)

	// Start while stopping: deferred
	require.NoError(t, loader.Start("stuck2", true))
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, st.preparedCount("stuck2"))

	// Stop finishes: the same monitor starts again
	st.mux.Lock()
	st.cleanup = func(string) {}
	st.mux.Unlock()
	releaseOnce.Do(func() { close(release) })
	waitPrepared(t, st, 2, "stuck2")

	// A stop that's no longer stuck returns nil
	assert.NoError(t, loader.Stop("stuck2", true))
	assert.NoError(t, loader.Unload("stuck2", true))
	assert.Empty(t, loader.Monitors())
}

func TestLoaderUnloadNotStarted(t *testing.T) {
	// Unloading a monitor that was loaded but never started must not call
	// Monitor.Stop, which panics on a monitor that never started.
	ids := []string{"never1"}
	st, loader := setupStopTest(t, "1s", &ids, func(blip.ConfigMonitor) bool { return false })
	defer status.RemoveMonitor("never1")

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	assert.NoError(t, loader.Stop("never1", true))

	setIds(st, &ids)
	require.NoError(t, loader.Load(context.Background()))
	assert.Empty(t, loader.Monitors())
	assert.Equal(t, 0, st.preparedCount("never1"))
}

func TestLoaderUnloadParallel(t *testing.T) {
	// Removing many monitors at once stops them in parallel. Compare one Load
	// that removes n monitors with stopping n monitors one at a time, which
	// is what Load did before. Each stop takes stopDelay in the collector
	// cleanup, standing in for the time a real monitor takes to stop.
	const n = 40
	const stopDelay = 50 * time.Millisecond

	ids := append(makeIds("par", n), makeIds("ser", n)...)
	st, loader := setupStopTest(t, "5s", &ids, nil)
	defer func() {
		for _, id := range append(makeIds("par", n), makeIds("ser", n)...) {
			status.RemoveMonitor(id)
		}
	}()
	st.cleanup = func(string) { time.Sleep(stopDelay) }

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, ids...)

	// Serial: stop and unload each "ser" monitor one at a time
	t0 := time.Now()
	for _, id := range makeIds("ser", n) {
		require.NoError(t, loader.Unload(id, true))
	}
	serial := time.Since(t0)

	// Parallel: one Load removes all the "par" monitors
	setIds(st, &ids)
	t0 = time.Now()
	require.NoError(t, loader.Load(context.Background()))
	parallel := time.Since(t0)
	assert.Empty(t, loader.Monitors())

	t.Logf("stop %d monitors (%s each): serial %s, parallel %s", n, stopDelay, serial, parallel)
	assert.GreaterOrEqual(t, serial, n*stopDelay)
	assert.Less(t, parallel, serial/4, "parallel stop not faster than serial stop")
	assert.Equal(t, 0, st.eventCount(event.MONITOR_STOP_TIMEOUT, ""))
}

func TestLoaderConcurrentStartStopLoad(t *testing.T) {
	// Hammer Load, StartMonitors, Start, and Stop concurrently (for the race
	// detector), then check that the Loader's state converges: every loaded
	// monitor can be started and is prepared, and unloading all of them stops
	// them without a stop timeout.
	all := makeIds("cc", 8)
	ids := append([]string{}, all...)
	st, loader := setupStopTest(t, "5s", &ids, nil)
	defer func() {
		for _, id := range all {
			status.RemoveMonitor(id)
		}
	}()
	st.cleanup = func(string) { time.Sleep(time.Millisecond) }

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				id := all[(w+i)%len(all)]
				switch (w + i) % 4 {
				case 0:
					// Drop half the monitors, or bring them all back
					if i%2 == 0 {
						setIds(st, &ids, all[:len(all)/2]...)
					} else {
						setIds(st, &ids, all...)
					}
					assert.NoError(t, loader.Load(context.Background()))
					loader.StartMonitors()
				case 1:
					assert.NoError(t, loader.Stop(id, true))
				case 2:
					err := loader.Start(id, true)
					if err != nil {
						assert.ErrorIs(t, err, monitor.ErrMonitorNotLoaded)
					}
				case 3:
					loader.StartMonitors()
				}
			}
		}(w)
	}
	wg.Wait()

	setIds(st, &ids, all...)
	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	assert.ElementsMatch(t, all, monitorIds(loader.Monitors()))

	setIds(st, &ids)
	require.NoError(t, loader.Load(context.Background()))
	assert.Empty(t, loader.Monitors())
	assert.Equal(t, 0, st.eventCount(event.MONITOR_STOP_TIMEOUT, ""))
}

func TestLoaderPendingStartRegistersSinkCounters(t *testing.T) {
	// A changed monitor is made (with new sinks) before the old monitor with
	// the same ID is unloaded. If the old monitor is still stopping, the new
	// monitor starts when it stops, right after the old monitor's status and
	// counters are removed. The new monitor's sink counter must be reported
	// and incremented after it starts.
	registerStopTestRetrySink.Do(func() {
		require.NoError(t, sink.Register(stopTestRetrySink, stopTestRetrySinkFactory{}))
	})
	const id = "sinkcounters1"
	counter := status.SINK_RETRY_DROPPED + ":" + mock.Sink{}.Name()
	ids := []string{id}
	st, loader := setupStopTest(t, "200ms", &ids, nil)
	defer status.RemoveMonitor(id)
	st.sinks = map[string]map[string]string{stopTestRetrySink: {"version": "1"}}

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	st.cleanup = func(string) { <-release } // block stop until released

	dropped := func() (uint64, bool) {
		v, ok := status.ReportCounters(id)[id][counter]
		return v, ok
	}

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, id)
	require.Eventually(t, func() bool { v, _ := dropped(); return v > 0 },
		5*time.Second, 5*time.Millisecond, "%s not incremented by first monitor", counter)

	// Change the sink config. Load makes the new monitor, then unloads the old
	// one, which is stuck stopping, so the new monitor's start is pending.
	st.mux.Lock()
	st.sinks = map[string]map[string]string{stopTestRetrySink: {"version": "2"}}
	st.mux.Unlock()
	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	assert.Contains(t, status.ReportMonitors(id)[id][status.MONITOR], "start pending")
	assert.Equal(t, 1, st.preparedCount(id), "new monitor started while old monitor was still stopping")

	// The old monitor stops, its status is removed, and the new monitor starts
	releaseOnce.Do(func() { close(release) })
	waitPrepared(t, st, 2, id)
	require.Eventually(t, func() bool { v, ok := dropped(); return ok && v > 0 },
		5*time.Second, 5*time.Millisecond, "%s not reported and incremented after new monitor started: %v", counter, status.ReportCounters(id))

	setIds(st, &ids)
	require.NoError(t, loader.Load(context.Background()))
}

// stopEvents returns the names of the stop events for a monitor, in order.
func (st *stopTest) stopEvents(monitorId string) []string {
	st.mux.Lock()
	defer st.mux.Unlock()
	names := []string{}
	for _, e := range st.events {
		if e.MonitorId != monitorId {
			continue
		}
		if e.Event == event.MONITOR_STOPPED || e.Event == event.MONITOR_STOP_TIMEOUT {
			names = append(names, e.Event)
		}
	}
	return names
}

func TestLoaderStopUnlocked(t *testing.T) {
	// Stop, Unload, and Start with lock=false are called by a caller that holds
	// the Loader mutex. Stop and Unload wait for the monitor to stop while the
	// caller holds the mutex, but no longer than the stop timeout.
	ids := []string{"unlocked1", "unlocked2"}
	st, loader := setupStopTest(t, "200ms", &ids, nil)
	defer status.RemoveMonitor("unlocked1")
	defer status.RemoveMonitor("unlocked2")

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var stuck sync.Map
	st.cleanup = func(monitorId string) {
		if _, ok := stuck.Load(monitorId); ok {
			<-release
		}
	}

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, "unlocked1", "unlocked2")

	// A normal stop returns nil once the monitor has stopped, and the stopped
	// monitor can be started again
	loader.Lock()
	assert.NoError(t, loader.Stop("unlocked1", false))
	assert.Contains(t, status.ReportMonitors("unlocked1")["unlocked1"][status.MONITOR], "stopped at")
	assert.NoError(t, loader.Start("unlocked1", false))
	loader.Unlock()
	waitPrepared(t, st, 2, "unlocked1")

	// A stuck stop returns ErrStopTimeout after the stop timeout
	stuck.Store("unlocked1", true)
	loader.Lock()
	t0 := time.Now()
	err := loader.Stop("unlocked1", false)
	d := time.Since(t0)
	loader.Unlock()
	assert.ErrorIs(t, err, monitor.ErrStopTimeout)
	assert.GreaterOrEqual(t, d, 200*time.Millisecond)
	assert.Less(t, d, 2*time.Second)
	assert.Eventually(t, func() bool { return st.eventCount(event.MONITOR_STOP_TIMEOUT, "unlocked1") == 1 },
		time.Second, 5*time.Millisecond)

	// Unload works the same: nil for a normal stop
	loader.Lock()
	assert.NoError(t, loader.Unload("unlocked2", false))
	loader.Unlock()
	assert.Equal(t, []string{"unlocked1"}, monitorIds(loader.Monitors()))

	releaseOnce.Do(func() { close(release) })
	loader.Lock()
	assert.NoError(t, loader.Unload("unlocked1", false))
	loader.Unlock()
	assert.Empty(t, loader.Monitors())
}

func TestLoaderStopEvents(t *testing.T) {
	// Every stop sends one MONITOR_STOPPED event, with the monitor ID. A stop
	// that misses the stop timeout sends MONITOR_STOP_TIMEOUT, then again every
	// 10 x the stop timeout, and MONITOR_STOPPED after all of them.
	ids := []string{"events1", "events2"}
	st, loader := setupStopTest(t, "50ms", &ids, nil)
	defer status.RemoveMonitor("events1")
	defer status.RemoveMonitor("events2")

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	st.cleanup = func(monitorId string) {
		if monitorId == "events2" {
			<-release
		}
	}

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, "events1", "events2")

	require.NoError(t, loader.Stop("events1", true))
	assert.Eventually(t, func() bool { return len(st.stopEvents("events1")) == 1 },
		time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{event.MONITOR_STOPPED}, st.stopEvents("events1"))

	// events2 is stuck: reported at 50ms and 550ms, then it stops at about 700ms
	assert.ErrorIs(t, loader.Stop("events2", true), monitor.ErrStopTimeout)
	require.Eventually(t, func() bool { return st.eventCount(event.MONITOR_STOP_TIMEOUT, "events2") == 2 },
		2*time.Second, 5*time.Millisecond)
	assert.Contains(t, stopStatus("events2"), "not stopped after 550ms")
	time.Sleep(150 * time.Millisecond)
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool { return len(st.stopEvents("events2")) == 3 },
		time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{event.MONITOR_STOP_TIMEOUT, event.MONITOR_STOP_TIMEOUT, event.MONITOR_STOPPED}, st.stopEvents("events2"))
	assert.Empty(t, stopStatus("events2"))

	// No MONITOR_STOPPED event without a monitor ID
	assert.Equal(t, 2, st.eventCount(event.MONITOR_STOPPED, ""))
}

func TestLoaderStopEventOrder(t *testing.T) {
	// A monitor that stops right when the stop timeout expires must not send
	// MONITOR_STOPPED before MONITOR_STOP_TIMEOUT. Run many monitors whose
	// stops take about as long as the stop timeout.
	const n = 20
	ids := makeIds("order", n)
	st, loader := setupStopTest(t, "20ms", &ids, nil)
	defer func() {
		for _, id := range makeIds("order", n) {
			status.RemoveMonitor(id)
		}
	}()
	st.cleanup = func(string) { time.Sleep(20 * time.Millisecond) }

	require.NoError(t, loader.Load(context.Background()))
	loader.StartMonitors()
	waitPrepared(t, st, 1, ids...)

	setIds(st, &ids)
	require.NoError(t, loader.Load(context.Background()))
	for _, id := range makeIds("order", n) {
		require.Eventually(t, func() bool { return st.eventCount(event.MONITOR_STOPPED, id) == 1 },
			2*time.Second, 5*time.Millisecond, "%s not stopped", id)
		got := st.stopEvents(id)
		assert.Equal(t, event.MONITOR_STOPPED, got[len(got)-1], "%s: stop events %v", id, got)
	}
}

func TestLoaderTryStart(t *testing.T) {
	// TryStart returns why a monitor did or didn't start
	ids := []string{"try1", "try-declined"}
	st, loader := setupStopTest(t, "100ms", &ids, func(cfg blip.ConfigMonitor) bool {
		return cfg.MonitorId != "try-declined"
	})
	defer status.RemoveMonitor("try1")
	defer status.RemoveMonitor("try-declined")

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	st.cleanup = func(string) { <-release }

	require.NoError(t, loader.Load(context.Background()))

	res, err := loader.TryStart("try1")
	require.NoError(t, err)
	assert.Equal(t, monitor.START_OK, res)
	waitPrepared(t, st, 1, "try1")

	res, err = loader.TryStart("try1")
	require.NoError(t, err)
	assert.Equal(t, monitor.START_RUNNING, res)

	res, err = loader.TryStart("try-declined")
	require.NoError(t, err)
	assert.Equal(t, monitor.START_DECLINED, res)

	res, err = loader.TryStart("not-loaded")
	assert.ErrorIs(t, err, monitor.ErrMonitorNotLoaded)
	assert.Equal(t, monitor.START_ERROR, res)

	// Stuck stop: start is pending, and stays pending if started again
	assert.ErrorIs(t, loader.Stop("try1", true), monitor.ErrStopTimeout)
	res, err = loader.TryStart("try1")
	require.NoError(t, err)
	assert.Equal(t, monitor.START_PENDING, res)
	res, err = loader.TryStart("try1")
	require.NoError(t, err)
	assert.Equal(t, monitor.START_PENDING, res)
	require.Eventually(t, func() bool {
		return strings.Contains(st.lastEventMsg(event.MONITOR_STOP_TIMEOUT, "try1"), "waiting for it to stop")
	}, 3*time.Second, 5*time.Millisecond, "stop timeout report doesn't mention the pending start")

	st.mux.Lock()
	st.cleanup = func(string) {}
	st.mux.Unlock()
	releaseOnce.Do(func() { close(release) })
	waitPrepared(t, st, 2, "try1")
	assert.NoError(t, loader.Unload("try1", true))
}
