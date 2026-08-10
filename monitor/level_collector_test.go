// Copyright 2024 Block, Inc.

package monitor_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

func init() {
	//blip.Debugging = true
}

var dbs = map[string]*sql.DB{}
var dbMaker = dbconn.NewConnFactory(nil, nil)
var pl = plan.NewLoader(nil)

func setup(t *testing.T, myVersion string) *sql.DB {
	db, ok := dbs[myVersion]
	if ok && db != nil {
		return db
	}
	_, db, err := test.Connection(myVersion)
	if err != nil {
		if test.Build {
			t.Skip(myVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	return db
}

func loadConfig(t *testing.T, planFile, monitorId, myVersion string) blip.ConfigMonitor {
	moncfg := blip.ConfigMonitor{
		MonitorId: monitorId,
		Username:  "root",
		Password:  "test",
		Hostname:  "127.0.0.1:" + test.MySQLPort[myVersion],
	}
	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{planFile}},
		Monitors: []blip.ConfigMonitor{moncfg},
	}
	moncfg.ApplyDefaults(cfg)
	if err := pl.LoadMonitor(moncfg, dbMaker); err != nil {
		t.Fatal(err)
	}
	return moncfg
}

func planSet(r event.Receiver) chan struct{} {
	c := make(chan struct{})
	er := mock.EventReceiver{
		RecvFunc: func(e event.Event) {
			if e.Event == event.CHANGE_PLAN_SUCCESS {
				close(c)
				blip.Debug("closed")
			}
		},
	}
	p := event.Tee{
		Receiver: er,
		Out:      r,
	}
	event.Subscribe(p)
	return c
}

// --------------------------------------------------------------------------

func TestLevelCollector(t *testing.T) {
	// Verify the most basic functionality of the LCO: that it collects each
	// level at the correct frequency. The test plan,
	//   planName := "../test/plans/lpc_1_5_10.yaml"
	// (below) has 3 levels at 1s, 5s, and 10s. But we're not going to wait
	// 10s for a test, so tickerDuration = 10 * time.Millisecond (below) changes
	// the ticks to 10ms, which produces the same effect: collect level 1
	// every tick (1s normally), collect level 2 every 5th tick (5s normally),
	// and collect level 3 every 10th tick (10s normally).

	// Create and register a mock blip.Collector that saves the level name
	// every time it's called. This is quite deep within the call stack,
	// which is what we want: LCO->engine->collector. By using a fake collector
	// but real LCO and engine , we testing the real, unmodified logic--
	// the LCO and engine don't know or care that this collector is a mock.
	db := setup(t, test.DefaultMySQLVersion)

	monitorId := "m1"
	defer status.RemoveMonitor(monitorId)

	mux := &sync.Mutex{}
	gotLevels := []string{}
	mc := mock.MetricsCollector{
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			mux.Lock()
			gotLevels = append(gotLevels, levelName)
			mux.Unlock()
			return nil, nil
		},
	}
	mf := mock.MetricFactory{
		MakeFunc: func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
			return mc, nil
		},
	}
	metrics.Register(mc.Domain(), mf) // MUST CALL FIRST, before the rest...
	defer metrics.Remove(mc.Domain())

	// Make a mini, fake config that uses the test plan and load it realistically
	// because the plan loader combines and sorts levels, etc. This is a lot of
	// boilerplate, but it ensure we test a realistic LCO and monitor--only the
	// collector is fake.
	planName := "../test/plans/lpc_1_5_10.yaml"
	moncfg := blip.ConfigMonitor{
		MonitorId: monitorId,
		Username:  "root",
		Password:  "test",
		Hostname:  "127.0.0.1:" + test.MySQLPort[test.DefaultMySQLVersion],
	}
	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{planName}},
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

	// Before running the LCO, change its internal ticker interval so we can test
	// this quickly rather than waiting a real 10s
	monitor.TickerDuration(10*time.Millisecond, time.Second)
	defer monitor.TickerDuration(time.Second, time.Second)

	// Create LCO and and run it, but it starts paused until ChangePlan is called
	// starts working once a plan is set.
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:     moncfg,
		DB:         db,
		PlanLoader: pl,
		Sinks:      []blip.Sink{mock.Sink{}},
	})
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go lco.Run(stopChan, doneChan)

	// Wait a few ticks and check LCO status to verify that is, in fact, paused
	time.Sleep(100 * time.Millisecond)
	s := status.ReportMonitors(monitorId)
	if !strings.Contains(s[monitorId][status.LEVEL_COLLECTOR], "paused") {
		t.Errorf("LCO not paused, expected it to be paused until ChangePlan is called (status=%+v)", s)
	}

	// ChangePlan sets the plan and un-pauses (starts collecting the new plan).
	// So call that and wait 15 ticks (150ms / 10s), then close the stopChan
	// to stop the LCO (don't leak goroutines)
	lco.ChangePlan(blip.STATE_ACTIVE, planName) // set plan, start collecting metrics
	time.Sleep(150 * time.Millisecond)
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for LCO to stop")
	}

	// LCO should have experienced 15s of running in 150ms because we set the
	// TickerDuration to 10ms (instead of its default 1s). That means the mock
	// collector should have been called 15 times, but CI systems can be really
	// slow, so we'll allow 15 or 16 calls.
	mux.Lock()
	if len(gotLevels) > 16 {
		t.Errorf("got %d levels, expected 15 or 16 ", len(gotLevels))
	}

	// If leveled collection is working properly, the first 12 levels collected--
	// as called by the LCO to engine.Collect--should be this sequence:
	if len(gotLevels) < 12 {
		t.Fatalf("got %d levels, expected at least 12", len(gotLevels))
	}
	expectLevels := []string{
		//            TICK  LEVEL
		"level_3", // 0s    1+2+3 (all levels)
		"level_1", // 1     1..
		"level_1", // 2     1..
		"level_1", // 3     1..
		"level_1", // 4     1..
		"level_2", // 5     .2.
		"level_1", // 6     1..
		"level_1", // 7     1..
		"level_1", // 8     1..
		"level_1", // 9     1..
		"level_3", // 10    ..3
		"level_1", // 11    1..
	}
	assert.ElementsMatch(t, gotLevels[:12], expectLevels)
	mux.Unlock()
}

func TestLevelCollector_SinkProcessing(t *testing.T) {
	// Verify that values collected are sent to the sink
	// in the order they were collected. None of the values
	// should be dropped or missed

	// Create and register a mock blip.Collector that saves the level name
	// every time it's called. This is quite deep within the call stack,
	// which is what we want: LCO->engine->collector. By using a fake collector
	// but real LCO and enginer, we testing the real, unmodified logic--
	// the LCO and engine don't know or care that this collector is a mock.
	db := setup(t, test.DefaultMySQLVersion)

	monitorId := "m1"
	defer status.RemoveMonitor(monitorId)

	mux := &sync.Mutex{}
	indexMux := &sync.Mutex{}
	index := 0
	gotMetrics := [][]blip.MetricValue{}

	metricValues := [][]blip.MetricValue{}
	for i := 0; i < 20; i++ {
		metricValues = append(metricValues, []blip.MetricValue{
			{
				Name:  "test-metric",
				Value: float64(i),
				Type:  blip.GAUGE,
			}})
	}

	mc := mock.MetricsCollector{
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			indexMux.Lock()
			defer indexMux.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			value := metricValues[index]
			index = index + 1
			if index >= len(metricValues) {
				index = 0
			}
			return value, nil
		},
	}
	mf := mock.MetricFactory{
		MakeFunc: func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
			return mc, nil
		},
	}
	mockSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			mux.Lock()
			gotMetrics = append(gotMetrics, m.Values["test"])
			mux.Unlock()
			return nil
		},
	}
	metrics.Register(mc.Domain(), mf) // MUST CALL FIRST, before the rest...
	defer metrics.Remove(mc.Domain())

	// Make a mini, fake config that uses the test plan and load it realistically
	// because the plan loader combines and sorts levels, etc. This is a lot of
	// boilerplate, but it ensure we test a realistic LCO and monitor--only the
	// collector is fake.
	planName := "../test/plans/lpc_1_5_10.yaml"
	moncfg := blip.ConfigMonitor{
		MonitorId: monitorId,
		Username:  "root",
		Password:  "test",
		Hostname:  "127.0.0.1" + test.MySQLPort[test.DefaultMySQLVersion],
	}
	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{planName}},
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

	// Before running the LCO, change its internal ticker interval so we can test
	// this quickly rather than waiting a real 10s
	monitor.TickerDuration(10*time.Millisecond, time.Second)
	defer monitor.TickerDuration(time.Second, time.Second)

	// Create LCO and and run it, but it starts paused until ChangePlan is called
	// starts working once a plan is set.
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:     moncfg,
		DB:         db,
		PlanLoader: pl,
		Sinks:      []blip.Sink{mockSink},
	})
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go lco.Run(stopChan, doneChan)

	// Wait a few ticks and check LCO status to verify that is, in fact, paused
	time.Sleep(100 * time.Millisecond)
	s := status.ReportMonitors(monitorId)
	if !strings.Contains(s[monitorId][status.LEVEL_COLLECTOR], "paused") {
		t.Errorf("LCO not paused, expected it to be paused until ChangePlan is called (status=%+v)", s)
	}

	// ChangePlan sets the plan and un-pauses (starts collecting the new plan).
	// So call that and wait 15 ticks (150ms / 10s), then close the stopChan
	// to stop the LCO (don't leak goroutines)
	lco.ChangePlan(blip.STATE_ACTIVE, planName) // set plan, start collecting metrics
	time.Sleep(150 * time.Millisecond)
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for LCO to stop")
	}

	// LCO should have experienced 15s of running in 150ms because we set the
	// TickerDuration to 10ms (instead of its default 1s). That means the mock
	// collector should have been called 15 times, but CI systems can be really
	// slow, so we'll allow 15 or 16 calls.
	mux.Lock()
	if len(gotMetrics) > 16 {
		t.Errorf("got %d metric values, expected 15 (exactxly) or 16 (at most)", len(gotMetrics))
	}

	// If leveled collection is working properly, the first 12 metric values collected--
	// as called by the LCO to engine.Collect--should be this sequence:
	if len(gotMetrics) < 12 {
		t.Fatalf("got %d metric values, expected at least 12", len(gotMetrics))
	}

	if diff := deep.Equal(gotMetrics[:12], metricValues[:12]); diff != nil {
		t.Error(diff)
	}
	mux.Unlock()
}

func TestLevelCollectorChangePlan(t *testing.T) {
	// ChangePlan is called async by LPA (if enabled), and ChangePlan runs a
	// goroutine (called changePlan) to handle it. When called again, it should
	// cancel the current changePlan goroutine, if any, then start a new one
	// for the new plan.
	//
	// To simulate, we need to make changePlan block, but it only does two things:
	// PlanLoader.Plan() to load the new plan, then Engine.Prepare() to prepare
	// the new plan. Neither have any direct callbacks or interfaces that we
	// can mock to make them slow (because these components are meant to be the
	// fastest and most efficient). However, Prepare() calls the same method on all
	// collectors, which we can mock. So we'll inject slowness in the callstack like:
	//
	//   4.   Collector.Prepare
	//   3.   Engine.Prepare
	//   2.   LCO.changePlan (goroutine)
	//   1. LCO.ChangePlan
	//   0. test

	db := setup(t, test.DefaultMySQLVersion)

	monitorId := "m2"
	defer status.RemoveMonitor(monitorId)

	callChan := make(chan bool, 1)
	returnChan := make(chan error, 1)
	mc := mock.MetricsCollector{
		PrepareFunc: func(ctx context.Context, plan blip.Plan) (func(), error) {
			blip.Debug("collector called")
			callChan <- true // signal test
			blip.Debug("collector waiting")
			err := <-returnChan // wait for test
			blip.Debug("collector return")
			return nil, err
		},
	}
	mf := mock.MetricFactory{
		MakeFunc: func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
			return mc, nil
		},
	}
	metrics.Register(mc.Domain(), mf) // MUST CALL FIRST, before the rest...
	defer metrics.Remove(mc.Domain())

	// Make a mini, fake config that uses the test plan and load it realistically
	planName := "../test/plans/test.yaml"
	moncfg := blip.ConfigMonitor{MonitorId: monitorId}
	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{planName}},
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

	// Create LCO and run it, but it starts paused until ChangePlan is called
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:     moncfg,
		DB:         db,
		PlanLoader: pl,
		Sinks:      []blip.Sink{mock.Sink{}},
	})
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go lco.Run(stopChan, doneChan)
	defer close(stopChan)

	// Call stack:
	//   4.   Collector.Prepare
	//   3.   Engine.Prepare
	//   2.   LCO.changePlan (goroutine)
	//   1. LCO.ChangePlan
	//   0. test

	// CP1: first change plan: returns immediately but the mock collector (mc) blocks on callChan
	lco.ChangePlan(blip.STATE_ACTIVE, planName)
	select {
	case <-callChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for CP1")
	}
	// CP1 is blocked in call stack 4

	// CP2: second change plan: cancels CP1, waits for CP1 to close its doneChan, then proceeds
	go lco.ChangePlan(blip.STATE_READ_ONLY, planName)

	// CP2 is blocked in call stack 2

	time.Sleep(100 * time.Millisecond)
	returnChan <- fmt.Errorf("fake context canceled error") // CP1 returns; CP2 advances to call stack 4

	select {
	case <-callChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for CP2")
	}

	returnChan <- nil // CP2 returns, which sets the state to READ_ONLY
	time.Sleep(150 * time.Millisecond)
	s := status.ReportMonitors(monitorId)
	if s[monitorId][status.LEVEL_STATE] != blip.STATE_READ_ONLY {
		t.Errorf("got state %s, expected %s", s[monitorId][status.LEVEL_STATE], blip.STATE_READ_ONLY)
	}
}

// --------------------------------------------------------------------------
// Red Green Blue plan tests
// --------------------------------------------------------------------------

func registerRGB(red, green, blue mock.MetricsCollector) mock.MetricFactory {
	mf := mock.MetricFactory{
		MakeFunc: func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
			switch domain {
			case "red":
				return red, nil
			case "green":
				return green, nil
			case "blue":
				return blue, nil
			default:
				return nil, fmt.Errorf("invalid test domain: %s", domain)
			}
		},
	}
	metrics.Register("red", mf)   // MUST CALL FIRST, before the rest...
	metrics.Register("green", mf) // MUST CALL FIRST, before the rest...
	metrics.Register("blue", mf)  // MUST CALL FIRST, before the rest...
	return mf
}

func TestLevelCollector_RGB_DomainPrioirty(t *testing.T) {
	// The test setup here is repeated in other TestLevelCollector_RGB tests
	// below. It's commented once here, not repeated in other tests because
	// it's virtually identical boilerplate setup.

	// Test domain priority: red is started first because it has the highest
	// frequency, then blue, then green. For stable test results, this requires:
	monitor.CollectParallel = 1
	defer func() { monitor.CollectParallel = 2 }()

	myVersion := test.DefaultMySQLVersion
	db := setup(t, myVersion)

	// Create and register fake red, blue, green domain collectors. This must be
	// done before loading the plan that refers to these domains. The mux is need
	// to avoid -test.race warnings.
	mux := &sync.Mutex{}
	gotLevels := []string{}

	red := mock.MetricsCollector{
		DomainFunc: func() string { return "red" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			mux.Lock()
			gotLevels = append(gotLevels, levelName+" red")
			mux.Unlock()
			return nil, nil
		},
	}
	green := mock.MetricsCollector{
		DomainFunc: func() string { return "green" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			mux.Lock()
			gotLevels = append(gotLevels, levelName+" green")
			mux.Unlock()
			return nil, nil
		},
	}
	blue := mock.MetricsCollector{
		DomainFunc: func() string { return "blue" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			mux.Lock()
			gotLevels = append(gotLevels, levelName+" blue")
			mux.Unlock()
			return nil, nil
		},
	}

	registerRGB(red, green, blue)
	defer func() {
		metrics.Remove("red")
		metrics.Remove("green")
		metrics.Remove("blue")
	}()

	// Load red-green-blue plan file which uses 100ms intervals
	plan := "../test/plans/rgb.yaml"
	moncfg := loadConfig(t, plan, "db1", myVersion)
	monitor.TickerDuration(100*time.Millisecond, 100*time.Millisecond)
	defer monitor.TickerDuration(time.Second, time.Second)

	// Create LCO and prime its stopChan for 5 iterations
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:     moncfg,
		DB:         db,
		PlanLoader: pl,
		Sinks:      []blip.Sink{},
	})
	stopChan := make(chan struct{}, 5)
	doneChan := make(chan struct{})
	for i := 0; i < 5; i++ {
		stopChan <- struct{}{}
	}
	close(stopChan)

	// readyChan is closed by a fake event receiver when lco.ChangePlan sends
	// event.CHANGE_PLAN_SUCCESS, which means the plan is loaded and ready.
	readyChan := planSet(nil)
	defer event.RemoveSubscribers()
	lco.ChangePlan(blip.STATE_ACTIVE, plan)
	<-readyChan

	// Run LCO and wait for it to finish (should be ~500ms for this test)
	go lco.Run(stopChan, doneChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for LCO to stop")
	}

	expectLevels := []string{
		"level_3 red", // interval 1
		"level_3 green",
		"level_3 blue",

		"level_1 red", // interval 2

		"level_2 red", // interval 3
		"level_2 green",

		"level_1 red", // interval 4

		"level_3 red", // interval 5
		"level_3 green",
		"level_3 blue",
	}
	mux.Lock()
	assert.ElementsMatch(t, gotLevels, expectLevels)
	mux.Unlock()
}

func TestLevelCollector_RGB_SlowBlue(t *testing.T) {
	// See TestLevelCollector_RGB_DomainPrioirty for comments on the common test
	// setup repeated below.

	// This test makes blue slow to verify that interval 1 does not block but,
	// rather, returns when its EMR expires after ~90ms.

	myVersion := test.DefaultMySQLVersion
	db := setup(t, myVersion)

	mux := &sync.Mutex{}
	mv := []blip.MetricValue{{Name: "", Value: 1}}
	red := mock.MetricsCollector{
		DomainFunc: func() string { return "red" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			return mv, nil
		},
	}
	green := mock.MetricsCollector{
		DomainFunc: func() string { return "green" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			return mv, nil
		},
	}

	blue := mock.MetricsCollector{
		DomainFunc: func() string { return "blue" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			blip.Debug("blue sleeps")
			time.Sleep(150 * time.Millisecond) // about half way into interval 2
			blip.Debug("blue awakes")
			return mv, nil
		},
	}

	registerRGB(red, green, blue)
	defer func() {
		metrics.Remove("red")
		metrics.Remove("green")
		metrics.Remove("blue")
	}()

	// TransformMetrics plugin
	reported := [][]string{}
	collectionTimes := [][]int64{}
	xf := func(metrics []*blip.Metrics) error {
		set := []string{}
		times := []int64{}
		for _, m := range metrics {
			for domain := range m.Values {
				set = append(set, fmt.Sprintf("%s %d %s", m.Level, m.Interval, domain))
			}
			times = append(times, m.End.Sub(m.Begin).Milliseconds())
		}
		mux.Lock()
		reported = append(reported, set)
		collectionTimes = append(collectionTimes, times)
		mux.Unlock()
		return nil
	}

	// Load red-green-blue plan file which uses a 100ms intervals
	plan := "../test/plans/rgb.yaml"
	moncfg := loadConfig(t, plan, "db1", myVersion)
	monitor.TickerDuration(100*time.Millisecond, 100*time.Millisecond)
	defer monitor.TickerDuration(time.Second, time.Second)

	// Create and run LCO for 4 intervals (collect blue just once)
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:           moncfg,
		DB:               db,
		PlanLoader:       pl,
		Sinks:            []blip.Sink{},
		TransformMetrics: xf,
	})
	stopChan := make(chan struct{}, 4)
	doneChan := make(chan struct{})
	for i := 0; i < 4; i++ {
		stopChan <- struct{}{}
	}
	close(stopChan)

	readyChan := planSet(nil)
	defer event.RemoveSubscribers()
	lco.ChangePlan(blip.STATE_ACTIVE, plan)
	<-readyChan

	go lco.Run(stopChan, doneChan)
	select {
	case <-doneChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for LCO to stop")
	}

	mux.Lock()
	defer mux.Unlock()

	/*
		Blue returns half way into interval 2, so it should be reported
		somewhere in interval 3. Collection is async, so it doesn't matter
		which order interval, only that it's there and nowhere else, like:

		"level_3 1 green", // interval 1
		"level_3 1 red"
		blue started here but ----------------+
			                                  |
		"level_1 2 red"    // interval 2      |
				                              |
		"level_2 3 green", // interval 3      |
		"level_2 3 red",                      |
		"level_3 1 blue"   collected here <---+

		"level_1 4 red"   // interval 4

		Also notice it's "level_3 1" (last number is the original interval)
	*/
	found := []int{}
	for i := range reported {
		for j := range reported[i] {
			if reported[i][j] == "level_3 1 blue" {
				found = append(found, i+1)
			}
		}
	}
	assert.ElementsMatch(t, found, []int{3})

	// Each interval is 100ms with a 10% buffer, so when blue doesn't return in
	// interval 1, the interval shouldn't block and show about a 90ms runtime.
	i1time := collectionTimes[0][0]
	if i1time < 88 || i1time > 92 {
		t.Errorf("interval 1 time %d ms, expected ~90 ms (88 < t < 92)", i1time)
	}
}

func TestLevelCollector_RGB_ProgressiveBlue(t *testing.T) {
	// See TestLevelCollector_RGB_DomainPrioirty for comments on the common test
	// setup repeated below.

	// This test makes blue return metrics every 50ms, which is half way into
	// each interval. That means it verifies 1) ErrMore works (don't block collection)
	// and 2) each interval sweeps up past/pending metrics.

	myVersion := test.DefaultMySQLVersion
	db := setup(t, myVersion)

	mux := &sync.Mutex{}
	mv := []blip.MetricValue{{Name: "", Value: 1}}
	nextInterval := make(chan bool, 1)
	red := mock.MetricsCollector{
		DomainFunc: func() string { return "red" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			nextInterval <- true
			return mv, nil
		},
	}
	green := mock.MetricsCollector{
		DomainFunc: func() string { return "green" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			return mv, nil
		},
	}

	callNo := 0
	interval := 0
	fgCallOK1 := false
	fgCallOK2 := false
	bgCallOK := false
	blue := mock.MetricsCollector{
		DomainFunc: func() string { return "blue" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			callNo += 1
			switch callNo {
			case 1:
				fgCallOK1 = ctx != nil && levelName != ""
				<-nextInterval // interval 1 (already done)
				interval += 1
				return nil, blip.ErrMore
			case 2:
				// first bg call immediately after callNo=1
				bgCallOK = ctx == nil && levelName == ""
				time.Sleep(50 * time.Millisecond) // half way into interval 1
				return mv, blip.ErrMore
				// interval 2 sweeps these values ^
			case 3:
				// second bg call
				<-nextInterval // interval 2
				<-nextInterval // interval 3
				<-nextInterval // interval 4
				blip.Debug("--- blue is done")
				// interval 5 will sweep these values before starting blue again:
				return mv, nil // done
			case 4:
				// second foreground call for interval 5 after flushing ^
				blip.Debug("--- blue called again")
				fgCallOK2 = ctx != nil && levelName != ""
				return mv, nil
			}
			return nil, fmt.Errorf("mock blue collector called too many times")
		},
	}

	registerRGB(red, green, blue)
	defer func() {
		metrics.Remove("red")
		metrics.Remove("green")
		metrics.Remove("blue")
	}()

	// TransformMetrics plugin
	reported := [][]string{}
	xf := func(metrics []*blip.Metrics) error {
		set := []string{}
		for _, m := range metrics {
			for domain := range m.Values {
				set = append(set, fmt.Sprintf("%s %d %s", m.Level, m.Interval, domain))
			}
		}
		mux.Lock()
		reported = append(reported, set)
		mux.Unlock()
		return nil
	}

	// Load red-green-blue plan file which uses a 100ms intervals
	plan := "../test/plans/rgb.yaml"
	moncfg := loadConfig(t, plan, "db1", myVersion)
	monitor.TickerDuration(100*time.Millisecond, 100*time.Millisecond)
	defer monitor.TickerDuration(time.Second, time.Second)

	// Create and run LCO for 4 intervals (collect blue just once)
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:           moncfg,
		DB:               db,
		PlanLoader:       pl,
		Sinks:            []blip.Sink{},
		TransformMetrics: xf,
	})
	stopChan := make(chan struct{}, 5)
	doneChan := make(chan struct{})
	for i := 0; i < 5; i++ {
		stopChan <- struct{}{}
	}
	close(stopChan)

	readyChan := planSet(nil)
	defer event.RemoveSubscribers()
	lco.ChangePlan(blip.STATE_ACTIVE, plan)
	<-readyChan

	go lco.Run(stopChan, doneChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for LCO to stop")
	}

	mux.Lock()
	defer mux.Unlock()

	if !fgCallOK1 {
		t.Errorf("first foreground Collect not called with ctx and level name")
	}
	if !bgCallOK {
		t.Errorf("background Collect not called with (nil, \"\") args")
	}
	if !fgCallOK2 {
		t.Errorf("second foreground Collect not called with ctx and level name")
	}

	/*
		Reports 2 and 5 are mixed: each starts with "level_3 1 blue"
		(1 is the interval) because blue reported metrics slowly.
		Important points are 1) we got the blue metrics and 2) the reports are
		sorted so that blue 1 is _before_ blue 5 in the last report, so sinks
		receive blue metrics in order.
			expectReported := [][]string{
				{"level_3 1 green", "level_3 1 red"},
				{"level_3 1 blue", "level_1 2 red"},
				{"level_2 3 red", "level_2 3 green"},
				{"level_1 4 red"},
				{"level_3 1 blue", "level_3 5 green", "level_3 5 red", "level_3 5 blue"},
			}
	*/
	if len(reported) != 5 {
		t.Fatalf("got %d reports, expected 5", len(reported))
	}
	if len(reported[1]) != 2 {
		t.Fatalf("got %d metrics in report[1], expected 2", len(reported[0]))
	}
	if len(reported[4]) != 4 {
		t.Fatalf("got %d metrics in report[4], expected 4", len(reported[4]))
	}

	if reported[1][0] != "level_3 1 blue" {
		t.Errorf("reported[1][0] = %s, expected 'level_3 1 blue'", reported[1][0])
	}
	if reported[4][0] != "level_3 1 blue" {
		t.Errorf("reported[4][0] = %s, expected 'level_3 1 blue'", reported[4][0])
	}
	blue2 := false
	for _, r := range reported[4][1:] {
		if r == "level_3 5 blue" {
			blue2 = true
		}
	}
	if !blue2 {
		t.Error("level_3 5 blue not repoted in interval 5")
	}
}

func TestLevelCollector_RGB_Fault(t *testing.T) {
	// See TestLevelCollector_RGB_DomainPrioirty for comments on the common test
	// setup repeated below.

	// This test makes RED fault: it doesn't stop before it's run again.
	// This causes the clutch to fence off the old interval so that when
	// the faulty collections return, they're discarded.

	myVersion := test.DefaultMySQLVersion
	db := setup(t, myVersion)

	mux := &sync.Mutex{}
	interval := 0
	blockChan := make(chan struct{})
	red := mock.MetricsCollector{
		DomainFunc: func() string { return "red" },
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			mux.Lock()
			interval += 1
			thisInterval := interval // local copy
			mux.Unlock()
			if thisInterval < 3 { // first 2 block, 3rd works
				blip.Debug("red %d is blocked", thisInterval)
				<-blockChan
				blip.Debug("red %d returns", thisInterval)
			}
			if thisInterval == 3 {
				close(blockChan)
			}
			return []blip.MetricValue{{Name: "int", Value: float64(thisInterval)}}, nil
		},
	}
	green := mock.MetricsCollector{ // do nothing, just return
		DomainFunc: func() string { return "green" },
	}
	blue := mock.MetricsCollector{ // do nothing, just return
		DomainFunc: func() string { return "blue" },
	}

	registerRGB(red, green, blue)
	defer func() {
		metrics.Remove("red")
		metrics.Remove("green")
		metrics.Remove("blue")
	}()

	// TransformMetrics plugin
	reported := [][]string{}
	xf := func(metrics []*blip.Metrics) error {
		set := []string{}
		for _, m := range metrics {
			for domain := range m.Values {
				set = append(set, fmt.Sprintf("%s %d %s", m.Level, m.Interval, domain))
			}
		}
		mux.Lock()
		reported = append(reported, set)
		mux.Unlock()
		return nil
	}

	// Load red-green-blue plan file which uses a 100ms intervals
	plan := "../test/plans/rgb.yaml"
	moncfg := loadConfig(t, plan, "db1", myVersion)
	monitor.TickerDuration(100*time.Millisecond, 100*time.Millisecond)
	defer monitor.TickerDuration(time.Second, time.Second)

	// Create and run LCO for 4 intervals (collect blue just once)
	lco := monitor.NewLevelCollector(monitor.LevelCollectorArgs{
		Config:           moncfg,
		DB:               db,
		PlanLoader:       pl,
		Sinks:            []blip.Sink{},
		TransformMetrics: xf,
	})
	stopChan := make(chan struct{}, 3)
	doneChan := make(chan struct{})
	for i := 0; i < 3; i++ {
		stopChan <- struct{}{}
	}
	close(stopChan)

	// Record events to check that 2 event.COLLECTOR_FAULT events are sent
	events := []string{}
	rec := mock.EventReceiver{
		RecvFunc: func(e event.Event) {
			events = append(events, e.Event)
		},
	}

	readyChan := planSet(rec)
	defer event.RemoveSubscribers()
	lco.ChangePlan(blip.STATE_ACTIVE, plan)
	<-readyChan

	go lco.Run(stopChan, doneChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for LCO to stop")
	}
	time.Sleep(100 * time.Millisecond)

	mux.Lock()
	defer mux.Unlock()
	expectReported := [][]string{
		{},
		{},
		{"level_2 3 red"},
	}
	assert.ElementsMatch(t, reported, expectReported)

	faults := 0
	drops := 0
	for _, e := range events {
		if e == event.COLLECTOR_FAULT {
			faults += 1
		}
		if e == event.DROP_METRICS_FENCE {
			drops += 1
		}
	}
	if faults != 2 {
		t.Errorf("got %d collector fault events, expected 2", faults)
	}
	if drops != 2 {
		t.Errorf("got %d metric drop events, expected 2", drops)
	}
}
