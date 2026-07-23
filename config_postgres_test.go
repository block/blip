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
	t.Setenv("BLIP_TEST_POSTGRES_DIAL_ADDRESS", "127.0.0.1:35432")

	monitor := blip.ConfigMonitor{
		MonitorId:      "postgres-monitor",
		DatabaseType:   "${BLIP_TEST_DATABASE_TYPE}",
		TimeoutConnect: "7s",
		Postgres: blip.ConfigPostgres{
			Database:        "${BLIP_TEST_POSTGRES_DATABASE}",
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
