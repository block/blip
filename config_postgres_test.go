// Copyright 2026 Block, Inc.

package blip_test

import (
	"strings"
	"testing"

	"github.com/cashapp/blip"
)

func TestConfigMonitorDatabaseTypeDefaultsToMySQLWithoutMutation(t *testing.T) {
	monitor := blip.ConfigMonitor{}
	monitor.ApplyDefaults(blip.DefaultConfig())

	if monitor.DatabaseType != "" {
		t.Fatalf("omitted database type mutated to %q", monitor.DatabaseType)
	}
	if monitor.EffectiveDatabaseType() != blip.DatabaseTypeMySQL {
		t.Fatalf("effective database type = %q, expected mysql", monitor.EffectiveDatabaseType())
	}
	if monitor.Postgres.Set() {
		t.Fatalf("PostgreSQL defaults added to MySQL monitor: %+v", monitor.Postgres)
	}
}

func TestConfigMonitorPostgresDefaultsAndInterpolation(t *testing.T) {
	t.Setenv("BLIP_TEST_DATABASE_TYPE", "postgres")
	t.Setenv("BLIP_TEST_POSTGRES_DATABASE", "metrics_database")
	t.Setenv("BLIP_TEST_POSTGRES_INCLUDE", "app_*")
	t.Setenv("BLIP_TEST_POSTGRES_DIAL_ADDRESS", "127.0.0.1:35432")

	monitor := blip.ConfigMonitor{
		MonitorId:      "postgres-monitor",
		DatabaseType:   "${BLIP_TEST_DATABASE_TYPE}",
		TimeoutConnect: "7s",
		Postgres: blip.ConfigPostgres{
			Database: "${BLIP_TEST_POSTGRES_DATABASE}",
			Databases: blip.ConfigPostgresDatabases{
				Include: []string{"${BLIP_TEST_POSTGRES_INCLUDE}"},
				Exclude: []string{"%{monitor.id}_scratch"},
			},
			ApplicationName: "%{monitor.id}",
			DialAddress:     "${BLIP_TEST_POSTGRES_DIAL_ADDRESS}",
		},
	}
	monitor.ApplyDefaults(blip.DefaultConfig())
	monitor.InterpolateEnvVars()
	monitor.InterpolateMonitor()

	if err := monitor.Validate(); err != nil {
		t.Fatal(err)
	}
	if monitor.DatabaseType != blip.DatabaseTypePostgres {
		t.Fatalf("database type = %q, expected postgres", monitor.DatabaseType)
	}
	if monitor.Postgres.Database != "metrics_database" {
		t.Fatalf("database = %q, expected metrics_database", monitor.Postgres.Database)
	}
	if monitor.Postgres.ApplicationName != monitor.MonitorId {
		t.Fatalf("application name = %q, expected monitor ID %q", monitor.Postgres.ApplicationName, monitor.MonitorId)
	}
	if monitor.Postgres.DialAddress != "127.0.0.1:35432" {
		t.Fatalf("dial address = %q, expected interpolated address", monitor.Postgres.DialAddress)
	}
	if monitor.Postgres.ConnectTimeout != "7s" {
		t.Fatalf("connect timeout = %q, expected inherited monitor timeout", monitor.Postgres.ConnectTimeout)
	}
	if monitor.Postgres.Databases.Enabled == nil || !*monitor.Postgres.Databases.Enabled {
		t.Fatalf("database discovery not enabled by default: %+v", monitor.Postgres.Databases.Enabled)
	}
	if got := monitor.Postgres.Databases.Include; len(got) != 1 || got[0] != "app_*" {
		t.Fatalf("database includes not interpolated: %#v", got)
	}
	if got := monitor.Postgres.Databases.Exclude; len(got) != 1 || got[0] != "postgres-monitor_scratch" {
		t.Fatalf("database excludes not monitor-interpolated: %#v", got)
	}
	if monitor.Postgres.Databases.Refresh != blip.DEFAULT_POSTGRES_DATABASE_REFRESH {
		t.Fatalf("database refresh = %q, expected %q", monitor.Postgres.Databases.Refresh, blip.DEFAULT_POSTGRES_DATABASE_REFRESH)
	}
	if monitor.Postgres.Databases.MaxConcurrency == nil ||
		*monitor.Postgres.Databases.MaxConcurrency != blip.DEFAULT_POSTGRES_DATABASE_MAX_CONCURRENCY {
		t.Fatalf("database max concurrency not defaulted: %+v", monitor.Postgres.Databases.MaxConcurrency)
	}
	if monitor.Postgres.MaxOpenConnections == nil || *monitor.Postgres.MaxOpenConnections != blip.DEFAULT_POSTGRES_MAX_OPEN_CONNECTIONS {
		t.Fatalf("max open connections not defaulted: %+v", monitor.Postgres.MaxOpenConnections)
	}
	if monitor.Postgres.MaxIdleConnections == nil || *monitor.Postgres.MaxIdleConnections != blip.DEFAULT_POSTGRES_MAX_IDLE_CONNECTIONS {
		t.Fatalf("max idle connections not defaulted: %+v", monitor.Postgres.MaxIdleConnections)
	}
}

func TestConfigMonitorDatabaseTypeValidation(t *testing.T) {
	tests := []struct {
		name      string
		monitor   blip.ConfigMonitor
		wantError string
	}{
		{
			name:      "unknown database type",
			monitor:   blip.ConfigMonitor{DatabaseType: "oracle"},
			wantError: "invalid database type",
		},
		{
			name: "PostgreSQL config on implicit MySQL monitor",
			monitor: blip.ConfigMonitor{
				Postgres: blip.ConfigPostgres{Database: "postgres"},
			},
			wantError: "requires database-type",
		},
		{
			name: "my.cnf on PostgreSQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseType: blip.DatabaseTypePostgres,
				MyCnf:        "/etc/blip/my.cnf",
			},
			wantError: "mycnf is only supported",
		},
		{
			name: "socket on PostgreSQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseType: blip.DatabaseTypePostgres,
				Socket:       "/tmp/.s.PGSQL.5432",
			},
			wantError: "socket is only supported",
		},
		{
			name: "heartbeat on PostgreSQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseType: blip.DatabaseTypePostgres,
				Heartbeat:    blip.ConfigHeartbeat{Freq: "1s"},
			},
			wantError: "heartbeat is only supported",
		},
		{
			name: "plan changing on PostgreSQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseType: blip.DatabaseTypePostgres,
				Plans: blip.ConfigPlans{
					Change: blip.ConfigPlanChange{
						Active: blip.ConfigStatePlan{Plan: "active"},
					},
				},
			},
			wantError: "plans.change is only supported",
		},
		{
			name: "plan change delay on PostgreSQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseType: blip.DatabaseTypePostgres,
				Plans: blip.ConfigPlans{
					Change: blip.ConfigPlanChange{
						Active: blip.ConfigStatePlan{After: "10s"},
					},
				},
			},
			wantError: "plans.change is only supported",
		},
		{
			name: "plan table on PostgreSQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseType: blip.DatabaseTypePostgres,
				Plans:        blip.ConfigPlans{Table: "blip.plans"},
			},
			wantError: "plans.table is only supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.monitor.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("got error %v, expected it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestConfigPlansRejectsPostgresTableMonitor(t *testing.T) {
	tests := []struct {
		name      string
		plans     blip.ConfigPlans
		wantError string
	}{
		{
			name: "implicit MySQL",
			plans: blip.ConfigPlans{
				Table:   "blip.plans",
				Monitor: &blip.ConfigMonitor{},
			},
		},
		{
			name: "PostgreSQL",
			plans: blip.ConfigPlans{
				Table: "blip.plans",
				Monitor: &blip.ConfigMonitor{
					DatabaseType: blip.DatabaseTypePostgres,
				},
			},
			wantError: "config.plans.table is only supported",
		},
		{
			name: "PostgreSQL without table",
			plans: blip.ConfigPlans{
				Monitor: &blip.ConfigMonitor{
					DatabaseType: blip.DatabaseTypePostgres,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plans.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("got error %v, expected it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestConfigMonitorDatabaseSpecificDefaults(t *testing.T) {
	defaults := blip.DefaultConfig()
	defaults.Heartbeat = blip.ConfigHeartbeat{
		Freq:  "1s",
		Table: "blip.heartbeat",
	}
	defaults.Plans = blip.ConfigPlans{
		Files: []string{"shared.yaml"},
		Change: blip.ConfigPlanChange{
			Active: blip.ConfigStatePlan{Plan: "active"},
		},
	}

	postgres := blip.ConfigMonitor{DatabaseType: blip.DatabaseTypePostgres}
	postgres.ApplyDefaults(defaults)
	if postgres.Heartbeat != (blip.ConfigHeartbeat{}) {
		t.Fatalf("PostgreSQL monitor inherited heartbeat defaults: %+v", postgres.Heartbeat)
	}
	if postgres.Plans.Change.Enabled() {
		t.Fatalf("PostgreSQL monitor inherited plan-changing defaults: %+v", postgres.Plans.Change)
	}
	if got := postgres.Plans.Files; len(got) != 1 || got[0] != "shared.yaml" {
		t.Fatalf("PostgreSQL monitor plan files = %#v, expected shared plan defaults", got)
	}
	if err := postgres.Validate(); err != nil {
		t.Fatalf("PostgreSQL monitor with shared defaults is invalid: %v", err)
	}

	mysql := blip.ConfigMonitor{}
	mysql.ApplyDefaults(defaults)
	if mysql.Heartbeat == (blip.ConfigHeartbeat{}) {
		t.Fatal("MySQL monitor did not inherit heartbeat defaults")
	}
	if !mysql.Plans.Change.Enabled() {
		t.Fatal("MySQL monitor did not inherit plan-changing defaults")
	}
}

func TestConfigPostgresAllowsExplicitUnlimitedPoolSettings(t *testing.T) {
	zero := 0
	config := blip.ConfigPostgres{
		Database:           "postgres",
		MaxOpenConnections: &zero,
		MaxIdleConnections: &zero,
	}
	config.ApplyDefaults(blip.DefaultConfigPostgres())

	if *config.MaxOpenConnections != 0 || *config.MaxIdleConnections != 0 {
		t.Fatalf("explicit zero pool settings were overwritten: %+v", config)
	}
}

func TestConfigPostgresValidation(t *testing.T) {
	minusOne := -1
	zero := 0
	one := 1
	two := 2
	tests := []struct {
		name      string
		config    blip.ConfigPostgres
		wantError string
	}{
		{
			name:      "invalid SSL mode",
			config:    blip.ConfigPostgres{SSLMode: "invalid"},
			wantError: "ssl-mode",
		},
		{
			name:      "negative max open",
			config:    blip.ConfigPostgres{MaxOpenConnections: &minusOne},
			wantError: "max-open-connections",
		},
		{
			name: "idle exceeds open",
			config: blip.ConfigPostgres{
				MaxOpenConnections: &one,
				MaxIdleConnections: &two,
			},
			wantError: "cannot exceed",
		},
		{
			name:      "zero connect timeout",
			config:    blip.ConfigPostgres{ConnectTimeout: "0"},
			wantError: "connect-timeout",
		},
		{
			name:      "invalid lifetime",
			config:    blip.ConfigPostgres{MaxConnectionLifetime: "tomorrow"},
			wantError: "max-connection-lifetime",
		},
		{
			name: "zero database concurrency",
			config: blip.ConfigPostgres{
				Databases: blip.ConfigPostgresDatabases{MaxConcurrency: &zero},
			},
			wantError: "databases.max-concurrency",
		},
		{
			name: "invalid database refresh",
			config: blip.ConfigPostgres{
				Databases: blip.ConfigPostgresDatabases{Refresh: "tomorrow"},
			},
			wantError: "databases.refresh",
		},
		{
			name: "empty database include",
			config: blip.ConfigPostgres{
				Databases: blip.ConfigPostgresDatabases{Include: []string{""}},
			},
			wantError: "databases.include",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("got error %v, expected it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestConfigPostgresDatabasesPreservesExplicitDisabled(t *testing.T) {
	disabled := false
	one := 1
	config := blip.ConfigPostgresDatabases{
		Enabled:        &disabled,
		Include:        []string{"app_*"},
		MaxConcurrency: &one,
	}
	config.ApplyDefaults(blip.DefaultConfigPostgresDatabases())

	if config.Enabled == nil || *config.Enabled {
		t.Fatalf("explicit disabled discovery was overwritten: %+v", config.Enabled)
	}
	if config.MaxConcurrency == nil || *config.MaxConcurrency != 1 {
		t.Fatalf("explicit concurrency was overwritten: %+v", config.MaxConcurrency)
	}
	if got := config.Include; len(got) != 1 || got[0] != "app_*" {
		t.Fatalf("explicit includes were overwritten: %#v", got)
	}
}
