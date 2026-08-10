// Copyright 2026 Block, Inc.

package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/heartbeat"
	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestMonitorMakeDBUsesProviderFactory(t *testing.T) {
	primary := &sql.DB{}
	provider := &testDBProvider{primary: primary}
	factory := &testDBProviderFactory{
		provider: provider,
		dsn:      "redacted",
	}
	monitor := NewMonitor(MonitorArgs{
		Config:  blip.ConfigMonitor{MonitorId: "provider"},
		DbMaker: factory,
	})

	gotProvider, gotPrimary, gotDSN, err := monitor.makeDB()
	if err != nil {
		t.Fatal(err)
	}
	if gotProvider != provider {
		t.Fatalf("provider = %T %p, expected %T %p", gotProvider, gotProvider, provider, provider)
	}
	if gotPrimary != primary {
		t.Fatalf("primary = %p, expected %p", gotPrimary, primary)
	}
	if gotDSN != "redacted" {
		t.Fatalf("DSN = %q, expected redacted", gotDSN)
	}
	if factory.makeCalls != 0 || factory.makeProviderCalls != 1 {
		t.Fatalf("factory calls: Make=%d MakeProvider=%d, expected 0 and 1",
			factory.makeCalls, factory.makeProviderCalls)
	}
}

func TestMonitorMakeDBPreservesLegacyFactory(t *testing.T) {
	primary := &sql.DB{}
	factory := &testLegacyDBFactory{primary: primary, dsn: "legacy"}
	monitor := NewMonitor(MonitorArgs{
		Config:  blip.ConfigMonitor{MonitorId: "legacy"},
		DbMaker: factory,
	})

	provider, gotPrimary, gotDSN, err := monitor.makeDB()
	if err != nil {
		t.Fatal(err)
	}
	if provider != nil {
		t.Fatalf("legacy factory returned provider %T, expected nil", provider)
	}
	if gotPrimary != primary || gotDSN != "legacy" {
		t.Fatalf("legacy result = (%p, %q), expected (%p, legacy)", gotPrimary, gotDSN, primary)
	}
}

func TestMonitorMakeDBRejectsInvalidProvider(t *testing.T) {
	tests := []struct {
		name       string
		provider   blip.DbProvider
		factoryErr error
		wantError  string
	}{
		{
			name:       "factory error",
			provider:   &testDBProvider{primary: &sql.DB{}},
			factoryErr: errors.New("make failed"),
			wantError:  "make failed",
		},
		{
			name:      "nil provider",
			wantError: "nil provider",
		},
		{
			name:      "typed nil provider",
			provider:  (*testDBProvider)(nil),
			wantError: "nil provider",
		},
		{
			name:       "factory error with typed nil provider",
			provider:   (*testDBProvider)(nil),
			factoryErr: errors.New("make failed"),
			wantError:  "make failed",
		},
		{
			name:      "nil primary",
			provider:  &testDBProvider{},
			wantError: "nil primary",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory := &testDBProviderFactory{
				provider: test.provider,
				err:      test.factoryErr,
			}
			monitor := NewMonitor(MonitorArgs{
				Config:  blip.ConfigMonitor{MonitorId: test.name},
				DbMaker: factory,
			})

			_, _, _, err := monitor.makeDB()
			if err == nil || !errors.Is(err, test.factoryErr) && !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("makeDB error = %v, expected %q", err, test.wantError)
			}
			if provider, ok := test.provider.(*testDBProvider); ok && provider != nil && provider.closeCalls != 1 {
				t.Fatalf("provider Close calls = %d, expected 1", provider.closeCalls)
			}
		})
	}
}

func TestMonitorCloseDBClosesProviderOnce(t *testing.T) {
	provider := &testDBProvider{primary: &sql.DB{}}
	monitor := &Monitor{
		db:         provider.primary,
		dbProvider: provider,
	}

	monitor.closeDB()
	monitor.closeDB()

	if provider.closeCalls != 1 {
		t.Fatalf("provider Close calls = %d, expected 1", provider.closeCalls)
	}
	if monitor.db != nil || monitor.dbProvider != nil {
		t.Fatalf("monitor retained database resources: db=%p provider=%T", monitor.db, monitor.dbProvider)
	}
}

func TestMonitorStopWaitsForPlanPreparationBeforeClosingProvider(t *testing.T) {
	TickerDuration(10*time.Millisecond, time.Second)
	defer TickerDuration(time.Second, time.Second)

	const (
		domain     = "test.provider-stop"
		monitorID  = "provider-stop"
		planName   = "provider-stop-plan"
		levelName  = "provider-stop-level"
		stopCaller = "provider-stop-test"
	)

	prepareStarted := make(chan struct{})
	prepareStopped := make(chan struct{})
	releasePrepare := make(chan struct{})
	collector := mock.MetricsCollector{
		DomainFunc: func() string { return domain },
		PrepareFunc: func(ctx context.Context, _ blip.Plan) (func(), error) {
			close(prepareStarted)
			select {
			case <-ctx.Done():
				close(prepareStopped)
				return nil, ctx.Err()
			case <-releasePrepare:
				close(prepareStopped)
				return nil, nil
			}
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

	loader := plan.NewLoader(func(blip.ConfigPlans) ([]blip.Plan, error) {
		return []blip.Plan{{
			Name: planName,
			Levels: map[string]blip.Level{
				levelName: {
					Name: levelName,
					Freq: "1s",
					Collect: map[string]blip.Domain{
						domain: {},
					},
				},
			},
		}}, nil
	})
	if err := loader.LoadShared(blip.ConfigPlans{}, nil); err != nil {
		t.Fatal(err)
	}
	config := blip.ConfigMonitor{MonitorId: monitorID}
	if err := loader.LoadMonitor(config, nil); err != nil {
		t.Fatal(err)
	}

	_, primary, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip("MySQL test fixture not running")
		}
		t.Fatal(err)
	}
	defer primary.Close()
	provider := &testDBProvider{
		primary: primary,
		closeFunc: func() {
			select {
			case <-prepareStopped:
			default:
				close(releasePrepare)
				t.Error("database provider closed before plan preparation stopped")
			}
		},
	}
	lco := newLevelCollectorWithDBProvider(LevelCollectorArgs{
		Config:     config,
		DB:         primary,
		PlanLoader: loader,
	}, provider)
	monitor := NewMonitor(MonitorArgs{Config: config})
	monitor.runChan = make(chan struct{})
	monitor.db = primary
	monitor.dbProvider = provider

	lcoDone := make(chan struct{})
	monitor.wg.Add(1)
	go func() {
		defer monitor.wg.Done()
		_ = lco.Run(monitor.runChan, lcoDone)
	}()
	if err := lco.ChangePlan(blip.STATE_ACTIVE, planName); err != nil {
		t.Fatal(err)
	}
	select {
	case <-prepareStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for plan preparation to start")
	}

	stopDone := make(chan struct{})
	go func() {
		monitor.stop(false, stopCaller)
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		select {
		case <-prepareStopped:
		default:
			close(releasePrepare)
		}
		t.Fatal("timeout waiting for monitor to stop")
	}

	select {
	case <-prepareStopped:
	default:
		t.Fatal("plan preparation was still running after monitor stop")
	}
	if provider.closeCalls != 1 {
		t.Fatalf("provider Close calls = %d, expected 1", provider.closeCalls)
	}
}

func TestMonitorStartupErrorStopsPartialSubsystemsBeforeClosingProvider(t *testing.T) {
	const (
		monitorID      = "partial-startup"
		exporterPlan   = "invalid-exporter"
		heartbeatDB    = "blip_monitor_provider_test"
		heartbeatTable = heartbeatDB + ".heartbeat"
	)

	_, primary, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip("MySQL test fixture not running")
		}
		t.Fatal(err)
	}
	defer primary.Close()
	if _, err := primary.Exec("CREATE DATABASE IF NOT EXISTS " + heartbeatDB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = primary.Exec("DROP DATABASE IF EXISTS " + heartbeatDB) })
	heartbeatDDL := strings.Replace(heartbeat.BLIP_TABLE_DDL, "heartbeat", heartbeatTable, 1)
	if _, err := primary.Exec(heartbeatDDL); err != nil {
		t.Fatal(err)
	}

	loader := plan.NewLoader(func(blip.ConfigPlans) ([]blip.Plan, error) {
		return []blip.Plan{{
			Name: exporterPlan,
			Levels: map[string]blip.Level{
				"fast": {Name: "fast", Freq: "1s"},
				"slow": {Name: "slow", Freq: "5s"},
			},
		}}, nil
	})
	if err := loader.LoadShared(blip.ConfigPlans{}, nil); err != nil {
		t.Fatal(err)
	}

	config := blip.ConfigMonitor{
		MonitorId: monitorID,
		Heartbeat: blip.ConfigHeartbeat{
			Freq:  "10ms",
			Table: heartbeatTable,
		},
		Exporter: blip.ConfigExporter{
			Mode: blip.EXPORTER_MODE_DUAL,
			Plan: exporterPlan,
		},
	}
	provider := &testDBProvider{primary: primary}
	monitor := NewMonitor(MonitorArgs{
		Config:     config,
		DbMaker:    &testDBProviderFactory{provider: provider, dsn: "redacted"},
		PlanLoader: loader,
	})
	monitor.runLoopChan = make(chan struct{})
	monitor.runChan = make(chan struct{})
	provider.closeFunc = func() {
		select {
		case <-monitor.runChan:
		default:
			t.Error("database provider closed before partial subsystems were stopped")
		}
	}

	err = monitor.startup()
	if err == nil || !strings.Contains(err.Error(), "expected 1") {
		t.Fatalf("startup error = %v, expected invalid exporter level count", err)
	}
	if provider.closeCalls != 1 {
		t.Fatalf("provider Close calls = %d, expected 1", provider.closeCalls)
	}
	if monitor.db != nil || monitor.dbProvider != nil {
		t.Fatalf("monitor retained database resources: db=%p provider=%T", monitor.db, monitor.dbProvider)
	}
}

func TestMonitorStopRunIgnoresObsoleteGeneration(t *testing.T) {
	oldRun := make(chan struct{})
	currentRun := make(chan struct{})
	monitor := NewMonitor(MonitorArgs{Config: blip.ConfigMonitor{MonitorId: "run-generation"}})
	monitor.runChan = currentRun

	monitor.stopRun(oldRun, "obsolete")

	select {
	case <-currentRun:
		t.Fatal("obsolete subsystem stopped the current run")
	default:
	}
}

func TestMonitorStopCleansExporterBeforeClosingProvider(t *testing.T) {
	cleaned := false
	provider := &testDBProvider{
		primary: &sql.DB{},
		closeFunc: func() {
			if !cleaned {
				t.Error("database provider closed before exporter cleanup")
			}
		},
	}
	collector := mock.MetricsCollector{DomainFunc: func() string { return "test.exporter-cleanup" }}
	engine := newEngineWithDBProvider(blip.ConfigMonitor{MonitorId: "exporter-cleanup"}, provider.primary, provider)
	engine.collectors[collector.Domain()] = &clutch{
		c:       collector,
		cleanup: func() { cleaned = true },
		domain:  collector.Domain(),
		Mutex:   &sync.Mutex{},
	}
	monitor := NewMonitor(MonitorArgs{Config: blip.ConfigMonitor{MonitorId: "exporter-cleanup"}})
	monitor.runChan = make(chan struct{})
	monitor.db = provider.primary
	monitor.dbProvider = provider
	monitor.exporter = NewExporter(blip.ConfigExporter{}, blip.Plan{}, engine)

	monitor.stop(false, "exporter-cleanup-test")

	if !cleaned {
		t.Fatal("exporter collector cleanup was not called")
	}
	if provider.closeCalls != 1 {
		t.Fatalf("provider Close calls = %d, expected 1", provider.closeCalls)
	}
}

func TestEngineStopWaitsForBackgroundCollector(t *testing.T) {
	const (
		domain = "test.background-stop"
		level  = "fast"
	)
	backgroundStarted := make(chan struct{})
	backgroundStopped := make(chan struct{})
	var collectCtx context.Context
	collector := mock.MetricsCollector{
		DomainFunc: func() string { return domain },
		CollectFunc: func(ctx context.Context, _ string) ([]blip.MetricValue, error) {
			if ctx != nil {
				collectCtx = ctx
				return nil, blip.ErrMore
			}
			close(backgroundStarted)
			<-collectCtx.Done()
			close(backgroundStopped)
			return nil, nil
		},
	}
	engine := newEngineWithDBProvider(blip.ConfigMonitor{MonitorId: "background-stop"}, &sql.DB{}, nil)
	cl := &clutch{
		c: collector,
		cleanup: func() {
			select {
			case <-backgroundStopped:
			default:
				t.Error("collector cleanup ran before background collection stopped")
			}
		},
		domain:         domain,
		cmr:            time.Second,
		collectionChan: engine.collectionChan,
		Mutex:          &sync.Mutex{},
	}
	engine.plan = blip.Plan{Name: "background", Levels: map[string]blip.Level{level: {Name: level, Freq: "1s"}}}
	engine.collectors[domain] = cl
	engine.collectAt[level] = []*clutch{cl}
	engine.checkAt[level] = nil

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := engine.Collect(ctx, 1, level, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backgroundStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for background collection to start")
	}

	engine.Stop()

	select {
	case <-backgroundStopped:
	default:
		t.Fatal("background collector was still running after Engine.Stop")
	}
}

func TestNewEngineWithDBProviderRetainsProvider(t *testing.T) {
	provider := &testDBProvider{primary: &sql.DB{}}
	engine := newEngineWithDBProvider(blip.ConfigMonitor{MonitorId: "engine"}, provider.primary, provider)

	if engine.DB() != provider.primary {
		t.Fatalf("engine primary = %p, expected %p", engine.DB(), provider.primary)
	}
	if engine.dbProvider != provider {
		t.Fatalf("engine provider = %T %p, expected %T %p", engine.dbProvider, engine.dbProvider, provider, provider)
	}
}

func TestEnginePassesProviderToCollectorFactory(t *testing.T) {
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip("MySQL test fixture not running")
		}
		t.Fatal(err)
	}
	defer db.Close()

	const domain = "test.db-provider"
	provider := &testDBProvider{primary: db}
	factory := &testProviderCollectorFactory{domain: domain}
	if err := metrics.Register(domain, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	engine := newEngineWithDBProvider(
		blip.ConfigMonitor{MonitorId: "provider-engine"},
		db,
		provider,
	)
	plan := blip.Plan{
		Name: "provider",
		Levels: map[string]blip.Level{
			"fast": {
				Name: "fast",
				Freq: "1s",
				Collect: map[string]blip.Domain{
					domain: {},
				},
			},
		},
	}
	if err := engine.Prepare(context.Background(), plan, func() {}, func() {}); err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	if factory.provider != provider {
		t.Fatalf("collector provider = %T %p, expected %T %p",
			factory.provider, factory.provider, provider, provider)
	}
	if factory.makeCalls != 0 || factory.makeProviderCalls != 1 {
		t.Fatalf("collector factory calls: Make=%d MakeWithDBProvider=%d, expected 0 and 1",
			factory.makeCalls, factory.makeProviderCalls)
	}
}

type testDBProvider struct {
	primary    *sql.DB
	closeCalls int
	closeFunc  func()
}

func (p *testDBProvider) Primary() *sql.DB {
	return p.primary
}

func (p *testDBProvider) Close() error {
	if p.closeFunc != nil {
		p.closeFunc()
	}
	p.closeCalls++
	return nil
}

type testDBProviderFactory struct {
	provider          blip.DbProvider
	dsn               string
	err               error
	makeCalls         int
	makeProviderCalls int
}

func (f *testDBProviderFactory) Make(blip.ConfigMonitor) (*sql.DB, string, error) {
	f.makeCalls++
	return nil, "", errors.New("legacy Make should not be called")
}

func (f *testDBProviderFactory) MakeProvider(blip.ConfigMonitor) (blip.DbProvider, string, error) {
	f.makeProviderCalls++
	return f.provider, f.dsn, f.err
}

type testLegacyDBFactory struct {
	primary *sql.DB
	dsn     string
}

func (f *testLegacyDBFactory) Make(blip.ConfigMonitor) (*sql.DB, string, error) {
	return f.primary, f.dsn, nil
}

type testProviderCollectorFactory struct {
	domain            string
	provider          blip.DbProvider
	makeCalls         int
	makeProviderCalls int
}

func (f *testProviderCollectorFactory) Make(string, blip.CollectorFactoryArgs) (blip.Collector, error) {
	f.makeCalls++
	return nil, errors.New("legacy Make should not be called")
}

func (f *testProviderCollectorFactory) MakeWithDBProvider(
	domain string,
	_ blip.CollectorFactoryArgs,
	provider blip.DbProvider,
) (blip.Collector, error) {
	f.makeProviderCalls++
	f.provider = provider
	if domain != f.domain {
		return nil, fmt.Errorf("collector domain = %q, expected %q", domain, f.domain)
	}
	return mock.MetricsCollector{DomainFunc: func() string { return domain }}, nil
}
