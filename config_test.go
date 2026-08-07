// Copyright 2024 Block, Inc.

package blip_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip/v2"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// --------------------------------------------------------------------------

func TestDefaultConfig(t *testing.T) {
	// The default config should be valid. Would be embarrassing if not.
	got := blip.DefaultConfig()
	if err := got.Validate(); err != nil {
		t.Errorf("default config is not valid, expected it to be valid: %s", err)
	}
}

// TestEnvInterpolation verifies that env vars with special characters, most notably $, are properly interpolated
func TestEnvInterpolation(t *testing.T) {
	envKey := "blip_test_TestEnvInterpolation"
	envVal := "a$1b!@#$%^&*()-+={};\""
	defer os.Unsetenv(envKey)

	err := os.Setenv(envKey, envVal)
	require.Nil(t, err)

	cfg := blip.Config{MySQL: blip.ConfigMySQL{Password: fmt.Sprintf("${%s}", envKey)}}
	cfg.InterpolateEnvVars()
	assert.Equal(t, envVal, cfg.MySQL.Password)
}

// TestEnvInterpolationEmpty verifies that a config like ${FOO:-bar} without FOO set, evaluates to "bar"
func TestEnvInterpolationEmpty(t *testing.T) {
	envKey := "blip_test_TestEnvInterpolation"
	_ = os.Unsetenv(envKey)

	cfg := blip.Config{MySQL: blip.ConfigMySQL{Password: fmt.Sprintf("${%s:-bar}", envKey)}}
	cfg.InterpolateEnvVars()
	assert.Equal(t, "bar", cfg.MySQL.Password)
}

func TestConfigRedacted(t *testing.T) {
	envKey := "blip_test_TestConfigRedacted"
	envPassword := "env-monitor-password"
	t.Setenv(envKey, envPassword)

	cfg := blip.Config{
		API: blip.ConfigAPI{Bind: "127.0.0.1:7522"},
		MySQL: blip.ConfigMySQL{
			Hostname: "defaults.example:3306",
			Username: "default-user",
			Password: "default-password",
		},
		Monitors: []blip.ConfigMonitor{
			{
				MonitorId: "inline-monitor",
				Hostname:  "inline.example:3306",
				Username:  "inline-user",
				Password:  "inline-monitor-password",
			},
			{
				MonitorId: "env-monitor",
				Hostname:  "env.example:3306",
				Username:  "env-user",
				Password:  fmt.Sprintf("${%s}", envKey),
			},
		},
		Plans: blip.ConfigPlans{
			Table: "blip.plans",
			Monitor: &blip.ConfigMonitor{
				MonitorId: "plan-monitor",
				Hostname:  "plans.example:3306",
				Username:  "plan-user",
				Password:  "plan-monitor-password",
			},
		},
	}
	cfg.Monitors[1].InterpolateEnvVars()

	redacted := cfg.Redacted()
	dump := fmt.Sprintf("%#v", redacted)

	for _, password := range []string{
		"default-password",
		"inline-monitor-password",
		envPassword,
		"plan-monitor-password",
	} {
		assert.NotContains(t, dump, password)
	}
	assert.Equal(t, "...", redacted.MySQL.Password)
	assert.Equal(t, "...", redacted.Monitors[0].Password)
	assert.Equal(t, "...", redacted.Monitors[1].Password)
	assert.Equal(t, "...", redacted.Plans.Monitor.Password)

	// Redaction retains useful non-secret context for debug logging.
	assert.Contains(t, dump, "127.0.0.1:7522")
	assert.Contains(t, dump, "inline-monitor")
	assert.Contains(t, dump, "inline.example:3306")
	assert.Contains(t, dump, "inline-user")
	assert.Contains(t, dump, "blip.plans")

	// Redaction must not change the live config.
	assert.Equal(t, "default-password", cfg.MySQL.Password)
	assert.Equal(t, "inline-monitor-password", cfg.Monitors[0].Password)
	assert.Equal(t, envPassword, cfg.Monitors[1].Password)
	assert.Equal(t, "plan-monitor-password", cfg.Plans.Monitor.Password)
}

func TestConfigMonitorRedactedPreservesPlanMonitorCycles(t *testing.T) {
	monitor := blip.ConfigMonitor{
		MonitorId: "cyclic-monitor",
		Password:  "cyclic-password",
	}
	monitor.Plans.Monitor = &monitor

	redacted := monitor.Redacted()

	assert.Equal(t, "...", redacted.Password)
	require.NotNil(t, redacted.Plans.Monitor)
	assert.Equal(t, "...", redacted.Plans.Monitor.Password)
	assert.Same(t, redacted.Plans.Monitor, redacted.Plans.Monitor.Plans.Monitor)
	assert.Equal(t, "cyclic-password", monitor.Password)
}

func TestConfigRedactedRedactsSinkOptionValues(t *testing.T) {
	envKey := "blip_test_TestConfigRedactedRedactsSinkOptionValues"
	envToken := "environment-expanded-sink-token"
	t.Setenv(envKey, envToken)

	cfg := blip.Config{
		Sinks: blip.ConfigSinks{
			"datadog": {
				"api-key-auth": "global-datadog-api-key",
				"api-compress": "global-non-secret-option-value",
			},
		},
		Monitors: []blip.ConfigMonitor{
			{
				MonitorId: "sink-monitor",
				Sinks: blip.ConfigSinks{
					"signalfx": {
						"auth-token": fmt.Sprintf("${%s}", envKey),
					},
				},
			},
		},
		Plans: blip.ConfigPlans{
			Monitor: &blip.ConfigMonitor{
				MonitorId: "plan-sink-monitor",
				Sinks: blip.ConfigSinks{
					"custom-sink": {
						"unknown-option": "custom-sink-option-value",
					},
				},
			},
		},
	}
	cfg.InterpolateEnvVars()
	cfg.Monitors[0].InterpolateEnvVars()
	cfg.Plans.Monitor.InterpolateEnvVars()

	redacted := cfg.Redacted()
	dump := fmt.Sprintf("%#v", redacted)

	for _, value := range []string{
		"global-datadog-api-key",
		"global-non-secret-option-value",
		envToken,
		"custom-sink-option-value",
	} {
		assert.NotContains(t, dump, value)
	}
	for _, optionName := range []string{
		"datadog",
		"api-key-auth",
		"api-compress",
		"signalfx",
		"auth-token",
	} {
		assert.Contains(t, dump, optionName)
	}
	assert.Equal(t, "...", redacted.Sinks["datadog"]["api-key-auth"])
	assert.Equal(t, "...", redacted.Monitors[0].Sinks["signalfx"]["auth-token"])
	assert.Equal(t, "...", redacted.Plans.Monitor.Sinks["custom-sink"]["unknown-option"])

	// Redaction must not change the live sink maps.
	assert.Equal(t, "global-datadog-api-key", cfg.Sinks["datadog"]["api-key-auth"])
	assert.Equal(t, envToken, cfg.Monitors[0].Sinks["signalfx"]["auth-token"])
	assert.Equal(t, "custom-sink-option-value", cfg.Plans.Monitor.Sinks["custom-sink"]["unknown-option"])
}

func TestConfigHTTPRedacted(t *testing.T) {
	envKey := "blip_test_TestConfigHTTPRedacted"
	t.Setenv(envKey, "environment-proxy-password")

	tests := []struct {
		name  string
		proxy string
		want  string
	}{
		{name: "empty"},
		{
			name:  "unauthenticated",
			proxy: "http://proxy.example:8080",
			want:  "http://proxy.example:8080",
		},
		{
			name:  "username only",
			proxy: "http://proxy-user@proxy.example:8080",
			want:  "http://proxy-user@proxy.example:8080",
		},
		{
			name:  "authenticated",
			proxy: "http://proxy-user:proxy-password@proxy.example:8080",
			want:  "http://proxy-user:xxxxx@proxy.example:8080",
		},
		{
			name:  "environment expanded password",
			proxy: fmt.Sprintf("http://proxy-user:${%s}@proxy.example:8080", envKey),
			want:  "http://proxy-user:xxxxx@proxy.example:8080",
		},
		{
			name:  "malformed",
			proxy: "http://proxy-user:%zz@proxy.example:8080",
			want:  "...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := blip.ConfigHTTP{Proxy: tt.proxy}
			cfg.InterpolateEnvVars()
			original := cfg.Proxy

			redacted := cfg.Redacted()

			assert.Equal(t, tt.want, redacted.Proxy)
			assert.Equal(t, original, cfg.Proxy, "redaction mutated the live HTTP config")
		})
	}
}

func TestApplyDefaultConfig(t *testing.T) {
	// Defaults apply when the value isn't set
	df := blip.DefaultConfig()
	my := blip.Config{}
	my.ApplyDefaults(df)
	if my.API.Bind != blip.DEFAULT_API_BIND {
		t.Errorf("api.bind=%s, expected %s", my.API.Bind, blip.DEFAULT_API_BIND)
	}

	// But when a value is set, it overrides the default
	my = blip.Config{
		API: blip.ConfigAPI{
			Bind: ":1234",
		},
	}
	my.ApplyDefaults(df)
	if my.API.Bind != ":1234" {
		t.Errorf("api.bind=%s, expected :1234", my.API.Bind)
	}
}
