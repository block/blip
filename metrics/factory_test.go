// Copyright 2026 Block, Inc.

package metrics_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/metrics"
	"github.com/cashapp/blip/test/mock"
)

type databaseTypesFactory struct {
	mock.MetricFactory
	databaseTypes func(string) []blip.DatabaseType
}

type providerFactory struct {
	mock.MetricFactory
	providerCalls int
	provider      blip.DbProvider
}

func (f *providerFactory) MakeWithDBProvider(
	domain string,
	args blip.CollectorFactoryArgs,
	provider blip.DbProvider,
) (blip.Collector, error) {
	f.providerCalls++
	f.provider = provider
	return f.MetricFactory.Make(domain, args)
}

type testProvider struct {
	primary *sql.DB
}

func (p testProvider) Primary() *sql.DB {
	return p.primary
}

func (testProvider) Close() error {
	return nil
}

func (f databaseTypesFactory) DatabaseTypes(domain string) []blip.DatabaseType {
	return f.databaseTypes(domain)
}

func TestRegisterDefaultsToMySQL(t *testing.T) {
	const domain = "test.legacy-mysql"
	factory := mock.MetricFactory{}

	if err := metrics.Register(domain, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	if err := metrics.ValidateDatabase(domain, blip.DatabaseTypeMySQL); err != nil {
		t.Fatalf("ValidateDatabase(mysql): %v", err)
	}
	if _, err := metrics.Make(domain, blip.CollectorFactoryArgs{}); err != nil {
		t.Fatalf("Make(default mysql): %v", err)
	}

	err := metrics.ValidateDatabase(domain, blip.DatabaseTypePostgres)
	if err == nil || !strings.Contains(err.Error(), `does not support database type "postgres" (supported: [mysql])`) {
		t.Fatalf("ValidateDatabase(postgres) error = %v", err)
	}
}

func TestRegisterUsesFactoryDatabaseTypes(t *testing.T) {
	const domain = "test.postgres-only"
	factory := databaseTypesFactory{
		databaseTypes: func(gotDomain string) []blip.DatabaseType {
			if gotDomain != domain {
				t.Fatalf("DatabaseTypes domain = %q, expected %q", gotDomain, domain)
			}
			return []blip.DatabaseType{blip.DatabaseTypePostgres}
		},
	}

	if err := metrics.Register(domain, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	if err := metrics.ValidateDatabase(domain, blip.DatabaseTypePostgres); err != nil {
		t.Fatalf("ValidateDatabase(postgres): %v", err)
	}
	if _, err := metrics.Make(domain, blip.CollectorFactoryArgs{
		Config: blip.ConfigMonitor{DatabaseType: blip.DatabaseTypePostgres},
	}); err != nil {
		t.Fatalf("Make(postgres): %v", err)
	}

	err := metrics.ValidateDatabase(domain, blip.DatabaseTypeMySQL)
	if err == nil || !strings.Contains(err.Error(), `does not support database type "mysql" (supported: [postgres])`) {
		t.Fatalf("ValidateDatabase(mysql) error = %v", err)
	}
	if _, err := metrics.Make(domain, blip.CollectorFactoryArgs{}); err == nil {
		t.Fatal("Make with the default MySQL database type succeeded")
	}

	// Global plan validation has no monitor database type. It must still be
	// able to construct the collector so shared plans can be loaded.
	if _, err := metrics.Make(domain, blip.CollectorFactoryArgs{Validate: true}); err != nil {
		t.Fatalf("Make(validate): %v", err)
	}
}

func TestRegisterSupportsMultipleDatabaseTypes(t *testing.T) {
	const domain = "test.multiple-database-types"
	factory := databaseTypesFactory{
		databaseTypes: func(string) []blip.DatabaseType {
			return []blip.DatabaseType{
				blip.DatabaseTypePostgres,
				blip.DatabaseTypeMySQL,
				blip.DatabaseTypePostgres,
			}
		},
	}

	if err := metrics.Register(domain, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	for _, databaseType := range []blip.DatabaseType{
		blip.DatabaseTypeMySQL,
		blip.DatabaseTypePostgres,
	} {
		if err := metrics.ValidateDatabase(domain, databaseType); err != nil {
			t.Fatalf("ValidateDatabase(%s): %v", databaseType, err)
		}
	}

	databaseTypes, err := metrics.SupportedDatabaseTypes(domain)
	if err != nil {
		t.Fatal(err)
	}
	if len(databaseTypes) != 2 ||
		databaseTypes[0] != blip.DatabaseTypeMySQL ||
		databaseTypes[1] != blip.DatabaseTypePostgres {
		t.Fatalf("SupportedDatabaseTypes = %v", databaseTypes)
	}

	// The registry owns its normalized compatibility metadata.
	databaseTypes[0] = "mutated"
	if err := metrics.ValidateDatabase(domain, blip.DatabaseTypeMySQL); err != nil {
		t.Fatalf("ValidateDatabase(mysql) after returned slice mutation: %v", err)
	}
}

func TestRegisterValidatesFactoryDatabaseTypes(t *testing.T) {
	tests := map[string]struct {
		databaseTypes []blip.DatabaseType
		errorContains string
	}{
		"empty": {
			databaseTypes: nil,
			errorContains: "supports no database types",
		},
		"invalid": {
			databaseTypes: []blip.DatabaseType{"oracle"},
			errorContains: `declares invalid database type "oracle"`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			domain := "test.invalid-database-types-" + name
			factory := databaseTypesFactory{
				databaseTypes: func(string) []blip.DatabaseType {
					return tt.databaseTypes
				},
			}

			err := metrics.Register(domain, factory)
			if err == nil || !strings.Contains(err.Error(), tt.errorContains) {
				t.Fatalf("Register error = %v", err)
			}
			if metrics.Exists(domain) {
				t.Fatalf("%s was registered", domain)
			}
		})
	}
}

func TestRegisterDuplicateDoesNotInspectFactoryDatabaseTypes(t *testing.T) {
	const domain = "test.duplicate-database-types"
	if err := metrics.Register(domain, mock.MetricFactory{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	factory := databaseTypesFactory{
		databaseTypes: func(string) []blip.DatabaseType {
			t.Fatal("DatabaseTypes called for duplicate registration")
			return nil
		},
	}
	err := metrics.Register(domain, factory)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("Register duplicate error = %v", err)
	}
}

func TestMakeWithDBProviderUsesOptionalFactoryCapability(t *testing.T) {
	const domain = "test.db-provider"
	makeCalls := 0
	factory := &providerFactory{
		MetricFactory: mock.MetricFactory{
			MakeFunc: func(string, blip.CollectorFactoryArgs) (blip.Collector, error) {
				makeCalls++
				return mock.MetricsCollector{}, nil
			},
		},
	}
	if err := metrics.Register(domain, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	provider := testProvider{primary: &sql.DB{}}
	if _, err := metrics.MakeWithDBProvider(domain, blip.CollectorFactoryArgs{}, provider); err != nil {
		t.Fatal(err)
	}
	if factory.providerCalls != 1 || makeCalls != 1 {
		t.Fatalf("factory calls: provider=%d Make=%d, expected 1 and 1",
			factory.providerCalls, makeCalls)
	}
	if factory.provider != provider {
		t.Fatalf("factory provider = %T %p, expected %T %p",
			factory.provider, factory.provider, provider, provider)
	}
}

func TestMakePreservesHistoricalFactoryPath(t *testing.T) {
	const domain = "test.db-provider-legacy-make"
	makeCalls := 0
	factory := &providerFactory{
		MetricFactory: mock.MetricFactory{
			MakeFunc: func(string, blip.CollectorFactoryArgs) (blip.Collector, error) {
				makeCalls++
				return mock.MetricsCollector{}, nil
			},
		},
	}
	if err := metrics.Register(domain, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Remove(domain) })

	if _, err := metrics.Make(domain, blip.CollectorFactoryArgs{}); err != nil {
		t.Fatal(err)
	}
	if factory.providerCalls != 0 || makeCalls != 1 {
		t.Fatalf("factory calls: provider=%d Make=%d, expected 0 and 1",
			factory.providerCalls, makeCalls)
	}
}

func TestBuiltInCollectorDatabaseCompatibility(t *testing.T) {
	if err := metrics.ValidateDatabase("status.global", blip.DatabaseTypeMySQL); err != nil {
		t.Fatalf("status.global with MySQL: %v", err)
	}
	if err := metrics.ValidateDatabase("status.global", blip.DatabaseTypePostgres); err == nil {
		t.Fatal("status.global supports PostgreSQL")
	}

	for _, databaseType := range []blip.DatabaseType{
		blip.DatabaseTypeMySQL,
		blip.DatabaseTypePostgres,
	} {
		if err := metrics.ValidateDatabase("aws.rds", databaseType); err != nil {
			t.Fatalf("aws.rds with %s: %v", databaseType, err)
		}
	}
}
