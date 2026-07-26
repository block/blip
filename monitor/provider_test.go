// Copyright 2026 Block, Inc.

package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/metrics"
	"github.com/cashapp/blip/test"
	"github.com/cashapp/blip/test/mock"
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
			if provider, ok := test.provider.(*testDBProvider); ok && provider.closeCalls != 1 {
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

func TestNewEngineWithProviderRetainsProvider(t *testing.T) {
	provider := &testDBProvider{primary: &sql.DB{}}
	engine := NewEngineWithProvider(blip.ConfigMonitor{MonitorId: "engine"}, provider.primary, provider)

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

	engine := NewEngineWithProvider(
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
}

func (p *testDBProvider) Primary() *sql.DB {
	return p.primary
}

func (p *testDBProvider) Close() error {
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
