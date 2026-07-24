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
	DEFAULT_POSTGRES_DATABASE_REFRESH         = "5m"
	DEFAULT_POSTGRES_DATABASE_MAX_CONCURRENCY = 4
)

// ConfigPostgres configures the PostgreSQL database/sql pool owned by one
// monitor. Credentials and TLS certificate files remain in the existing
// monitor-level fields so all Blip credential sources can be shared by
// database-specific connection factories.
type ConfigPostgres struct {
	Database              string                  `yaml:"database,omitempty"`
	Databases             ConfigPostgresDatabases `yaml:"databases,omitempty"`
	ApplicationName       string                  `yaml:"application-name,omitempty"`
	SSLMode               string                  `yaml:"ssl-mode,omitempty"`
	MaxOpenConnections    *int                    `yaml:"max-open-connections,omitempty"`
	MaxIdleConnections    *int                    `yaml:"max-idle-connections,omitempty"`
	MaxConnectionIdleTime string                  `yaml:"max-connection-idle-time,omitempty"`
	MaxConnectionLifetime string                  `yaml:"max-connection-lifetime,omitempty"`
	ConnectTimeout        string                  `yaml:"connect-timeout,omitempty"`
	StatementTimeout      string                  `yaml:"statement-timeout,omitempty"`
	LockTimeout           string                  `yaml:"lock-timeout,omitempty"`
	DialAddress           string                  `yaml:"dial-address,omitempty"`
}

// ConfigPostgresDatabases selects the databases that database-local
// PostgreSQL collectors monitor. An empty Include selects every eligible
// database, and Exclude patterns always take precedence. Patterns are
// case-sensitive and support * and ? wildcards.
type ConfigPostgresDatabases struct {
	Enabled        *bool    `yaml:"enabled,omitempty"`
	Include        []string `yaml:"include,omitempty"`
	Exclude        []string `yaml:"exclude,omitempty"`
	Refresh        string   `yaml:"refresh,omitempty"`
	MaxConcurrency *int     `yaml:"max-concurrency,omitempty"`
}

func DefaultConfigPostgres() ConfigPostgres {
	return ConfigPostgres{
		Database:              DEFAULT_POSTGRES_DATABASE,
		Databases:             DefaultConfigPostgresDatabases(),
		ApplicationName:       DEFAULT_POSTGRES_APPLICATION_NAME,
		MaxOpenConnections:    postgresInt(DEFAULT_POSTGRES_MAX_OPEN_CONNECTIONS),
		MaxIdleConnections:    postgresInt(DEFAULT_POSTGRES_MAX_IDLE_CONNECTIONS),
		MaxConnectionIdleTime: DEFAULT_POSTGRES_MAX_CONNECTION_IDLE_TIME,
		MaxConnectionLifetime: DEFAULT_POSTGRES_MAX_CONNECTION_LIFETIME,
		ConnectTimeout:        DEFAULT_MONITOR_TIMEOUT_CONNECT,
	}
}

func DefaultConfigPostgresDatabases() ConfigPostgresDatabases {
	return ConfigPostgresDatabases{
		Enabled:        postgresBool(true),
		Refresh:        DEFAULT_POSTGRES_DATABASE_REFRESH,
		MaxConcurrency: postgresInt(DEFAULT_POSTGRES_DATABASE_MAX_CONCURRENCY),
	}
}

// Set reports whether a monitor explicitly contains PostgreSQL configuration.
func (c ConfigPostgres) Set() bool {
	return c.Database != "" ||
		c.Databases.Set() ||
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
	c.Databases.ApplyDefaults(defaults.Databases)
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

func (c ConfigPostgresDatabases) Set() bool {
	return c.Enabled != nil ||
		c.Include != nil ||
		c.Exclude != nil ||
		c.Refresh != "" ||
		c.MaxConcurrency != nil
}

func (c *ConfigPostgresDatabases) ApplyDefaults(defaults ConfigPostgresDatabases) {
	if c.Enabled == nil && defaults.Enabled != nil {
		c.Enabled = postgresBool(*defaults.Enabled)
	}
	if c.Include == nil && defaults.Include != nil {
		c.Include = append([]string(nil), defaults.Include...)
	}
	if c.Exclude == nil && defaults.Exclude != nil {
		c.Exclude = append([]string(nil), defaults.Exclude...)
	}
	if c.Refresh == "" {
		c.Refresh = defaults.Refresh
	}
	c.MaxConcurrency = setPostgresInt(c.MaxConcurrency, defaults.MaxConcurrency)
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
	if err := validatePostgresDuration("lock-timeout", c.LockTimeout, true); err != nil {
		return err
	}
	return c.Databases.Validate()
}

func (c ConfigPostgresDatabases) Validate() error {
	if err := validatePostgresDuration("databases.refresh", c.Refresh, false); err != nil {
		return err
	}
	if c.MaxConcurrency != nil && *c.MaxConcurrency <= 0 {
		return fmt.Errorf("config.postgres.databases.max-concurrency: must be greater than zero")
	}
	for _, patterns := range []struct {
		name   string
		values []string
	}{
		{name: "include", values: c.Include},
		{name: "exclude", values: c.Exclude},
	} {
		for _, pattern := range patterns.values {
			if pattern == "" {
				return fmt.Errorf("config.postgres.databases.%s: patterns cannot be empty", patterns.name)
			}
		}
	}
	return nil
}

func postgresBool(value bool) *bool {
	return &value
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
	c.Databases.InterpolateEnvVars()
	c.ApplicationName = interpolateEnv(c.ApplicationName)
	c.SSLMode = interpolateEnv(c.SSLMode)
	c.MaxConnectionIdleTime = interpolateEnv(c.MaxConnectionIdleTime)
	c.MaxConnectionLifetime = interpolateEnv(c.MaxConnectionLifetime)
	c.ConnectTimeout = interpolateEnv(c.ConnectTimeout)
	c.StatementTimeout = interpolateEnv(c.StatementTimeout)
	c.LockTimeout = interpolateEnv(c.LockTimeout)
	c.DialAddress = interpolateEnv(c.DialAddress)
}

func (c *ConfigPostgresDatabases) InterpolateEnvVars() {
	for i := range c.Include {
		c.Include[i] = interpolateEnv(c.Include[i])
	}
	for i := range c.Exclude {
		c.Exclude[i] = interpolateEnv(c.Exclude[i])
	}
	c.Refresh = interpolateEnv(c.Refresh)
}

func (c *ConfigPostgres) InterpolateMonitor(m *ConfigMonitor) {
	c.Database = m.interpolateMon(c.Database)
	c.Databases.InterpolateMonitor(m)
	c.ApplicationName = m.interpolateMon(c.ApplicationName)
	c.SSLMode = m.interpolateMon(c.SSLMode)
	c.MaxConnectionIdleTime = m.interpolateMon(c.MaxConnectionIdleTime)
	c.MaxConnectionLifetime = m.interpolateMon(c.MaxConnectionLifetime)
	c.ConnectTimeout = m.interpolateMon(c.ConnectTimeout)
	c.StatementTimeout = m.interpolateMon(c.StatementTimeout)
	c.LockTimeout = m.interpolateMon(c.LockTimeout)
	c.DialAddress = m.interpolateMon(c.DialAddress)
}

func (c *ConfigPostgresDatabases) InterpolateMonitor(m *ConfigMonitor) {
	for i := range c.Include {
		c.Include[i] = m.interpolateMon(c.Include[i])
	}
	for i := range c.Exclude {
		c.Exclude[i] = m.interpolateMon(c.Exclude[i])
	}
	c.Refresh = m.interpolateMon(c.Refresh)
}
