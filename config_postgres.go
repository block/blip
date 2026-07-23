// Copyright 2026 Block, Inc.

package blip

import (
	"fmt"
	"strings"
	"time"
)

const (
	DEFAULT_POSTGRES_DATABASE                 = "postgres"
	DEFAULT_POSTGRES_APPLICATION_NAME         = "pgblip"
	DEFAULT_POSTGRES_MAX_OPEN_CONNECTIONS     = 4
	DEFAULT_POSTGRES_MAX_IDLE_CONNECTIONS     = 2
	DEFAULT_POSTGRES_MAX_CONNECTION_IDLE_TIME = "30s"
	DEFAULT_POSTGRES_MAX_CONNECTION_LIFETIME  = "0"
)

// ConfigPostgres configures the PostgreSQL database/sql pool owned by one
// monitor. Credentials and TLS certificate files remain in the existing
// monitor-level fields so all Blip credential sources can be shared by
// database-specific connection factories.
type ConfigPostgres struct {
	Database              string `yaml:"database,omitempty"`
	ApplicationName       string `yaml:"application-name,omitempty"`
	SSLMode               string `yaml:"ssl-mode,omitempty"`
	MaxOpenConnections    *int   `yaml:"max-open-connections,omitempty"`
	MaxIdleConnections    *int   `yaml:"max-idle-connections,omitempty"`
	MaxConnectionIdleTime string `yaml:"max-connection-idle-time,omitempty"`
	MaxConnectionLifetime string `yaml:"max-connection-lifetime,omitempty"`
	ConnectTimeout        string `yaml:"connect-timeout,omitempty"`
	StatementTimeout      string `yaml:"statement-timeout,omitempty"`
	LockTimeout           string `yaml:"lock-timeout,omitempty"`
	DialAddress           string `yaml:"dial-address,omitempty"`
}

func DefaultConfigPostgres() ConfigPostgres {
	return ConfigPostgres{
		Database:              DEFAULT_POSTGRES_DATABASE,
		ApplicationName:       DEFAULT_POSTGRES_APPLICATION_NAME,
		MaxOpenConnections:    postgresInt(DEFAULT_POSTGRES_MAX_OPEN_CONNECTIONS),
		MaxIdleConnections:    postgresInt(DEFAULT_POSTGRES_MAX_IDLE_CONNECTIONS),
		MaxConnectionIdleTime: DEFAULT_POSTGRES_MAX_CONNECTION_IDLE_TIME,
		MaxConnectionLifetime: DEFAULT_POSTGRES_MAX_CONNECTION_LIFETIME,
		ConnectTimeout:        DEFAULT_MONITOR_TIMEOUT_CONNECT,
	}
}

// Set reports whether a monitor explicitly contains PostgreSQL configuration.
func (c ConfigPostgres) Set() bool {
	return c.Database != "" ||
		c.ApplicationName != "" ||
		c.SSLMode != "" ||
		c.MaxOpenConnections != nil ||
		c.MaxIdleConnections != nil ||
		c.MaxConnectionIdleTime != "" ||
		c.MaxConnectionLifetime != "" ||
		c.ConnectTimeout != "" ||
		c.StatementTimeout != "" ||
		c.LockTimeout != "" ||
		c.DialAddress != ""
}

func (c *ConfigPostgres) ApplyDefaults(defaults ConfigPostgres) {
	if c.Database == "" {
		c.Database = defaults.Database
	}
	if c.ApplicationName == "" {
		c.ApplicationName = defaults.ApplicationName
	}
	if c.SSLMode == "" {
		c.SSLMode = defaults.SSLMode
	}
	c.MaxOpenConnections = setPostgresInt(c.MaxOpenConnections, defaults.MaxOpenConnections)
	c.MaxIdleConnections = setPostgresInt(c.MaxIdleConnections, defaults.MaxIdleConnections)
	if c.MaxConnectionIdleTime == "" {
		c.MaxConnectionIdleTime = defaults.MaxConnectionIdleTime
	}
	if c.MaxConnectionLifetime == "" {
		c.MaxConnectionLifetime = defaults.MaxConnectionLifetime
	}
	if c.ConnectTimeout == "" {
		c.ConnectTimeout = defaults.ConnectTimeout
	}
	if c.StatementTimeout == "" {
		c.StatementTimeout = defaults.StatementTimeout
	}
	if c.LockTimeout == "" {
		c.LockTimeout = defaults.LockTimeout
	}
	if c.DialAddress == "" {
		c.DialAddress = defaults.DialAddress
	}
}

func (c ConfigPostgres) Validate() error {
	validSSLModes := map[string]bool{
		"":            true,
		"disable":     true,
		"allow":       true,
		"prefer":      true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
	}
	if !validSSLModes[strings.ToLower(c.SSLMode)] {
		return fmt.Errorf("config.postgres.ssl-mode: invalid PostgreSQL SSL mode %q", c.SSLMode)
	}
	if c.MaxOpenConnections != nil && *c.MaxOpenConnections < 0 {
		return fmt.Errorf("config.postgres.max-open-connections: must be greater than or equal to zero")
	}
	if c.MaxIdleConnections != nil && *c.MaxIdleConnections < 0 {
		return fmt.Errorf("config.postgres.max-idle-connections: must be greater than or equal to zero")
	}
	if c.MaxOpenConnections != nil && c.MaxIdleConnections != nil &&
		*c.MaxOpenConnections > 0 && *c.MaxIdleConnections > *c.MaxOpenConnections {
		return fmt.Errorf("config.postgres.max-idle-connections: cannot exceed max-open-connections")
	}
	if err := validatePostgresDuration("connect-timeout", c.ConnectTimeout, false); err != nil {
		return err
	}
	if err := validatePostgresDuration("max-connection-idle-time", c.MaxConnectionIdleTime, true); err != nil {
		return err
	}
	if err := validatePostgresDuration("max-connection-lifetime", c.MaxConnectionLifetime, true); err != nil {
		return err
	}
	if err := validatePostgresDuration("statement-timeout", c.StatementTimeout, true); err != nil {
		return err
	}
	return validatePostgresDuration("lock-timeout", c.LockTimeout, true)
}

func postgresInt(value int) *int {
	return &value
}

func setPostgresInt(value, defaultValue *int) *int {
	if value != nil || defaultValue == nil {
		return value
	}
	copy := *defaultValue
	return &copy
}

func validatePostgresDuration(name, value string, allowZero bool) error {
	if value == "" {
		return nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("config.postgres.%s: invalid duration %q: %w", name, value, err)
	}
	if duration < 0 || (!allowZero && duration == 0) {
		constraint := "greater than zero"
		if allowZero {
			constraint = "greater than or equal to zero"
		}
		return fmt.Errorf("config.postgres.%s: must be %s", name, constraint)
	}
	return nil
}

func (c *ConfigPostgres) InterpolateEnvVars() {
	c.Database = interpolateEnv(c.Database)
	c.ApplicationName = interpolateEnv(c.ApplicationName)
	c.SSLMode = interpolateEnv(c.SSLMode)
	c.MaxConnectionIdleTime = interpolateEnv(c.MaxConnectionIdleTime)
	c.MaxConnectionLifetime = interpolateEnv(c.MaxConnectionLifetime)
	c.ConnectTimeout = interpolateEnv(c.ConnectTimeout)
	c.StatementTimeout = interpolateEnv(c.StatementTimeout)
	c.LockTimeout = interpolateEnv(c.LockTimeout)
	c.DialAddress = interpolateEnv(c.DialAddress)
}

func (c *ConfigPostgres) InterpolateMonitor(m *ConfigMonitor) {
	c.Database = m.interpolateMon(c.Database)
	c.ApplicationName = m.interpolateMon(c.ApplicationName)
	c.SSLMode = m.interpolateMon(c.SSLMode)
	c.MaxConnectionIdleTime = m.interpolateMon(c.MaxConnectionIdleTime)
	c.MaxConnectionLifetime = m.interpolateMon(c.MaxConnectionLifetime)
	c.ConnectTimeout = m.interpolateMon(c.ConnectTimeout)
	c.StatementTimeout = m.interpolateMon(c.StatementTimeout)
	c.LockTimeout = m.interpolateMon(c.LockTimeout)
	c.DialAddress = m.interpolateMon(c.DialAddress)
}
