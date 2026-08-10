// Copyright 2024 Block, Inc.

package blip

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	DEFAULT_CONFIG_FILE = "blip.yaml"
	DEFAULT_DATABASE    = "blip"
)

var envvar = regexp.MustCompile(`\${([\w_.-]+)(?:(\:\-)([\w_.-]*))?}`)

// interpolateEnv changes ${FOO} to the value of environment variable FOO.
// It also changes ${FOO:-bar} to "bar" if env var FOO is an empty string.
// Only strict matching, else the original string is returned.
func interpolateEnv(v string) string {
	if !strings.Contains(v, "${") {
		return v
	}
	return envvar.ReplaceAllStringFunc(v, func(match string) string {
		m := envvar.FindStringSubmatch(match)
		if len(m) != 4 {
			return match // strict match only
		}
		value := os.Getenv(m[1])
		if value == "" && m[2] != "" {
			return m[3]
		}
		return value
	})
}

// setBool sets c to the value of b if c is nil (not set). Pointers are required
// because var b bool is false by default whether set or not, so we can't tell if
// it's explicitly set in config file. But var b *bool is only false if explicitly
// set b=false, else it's nil, so no we can tell if the var is set or not.
func setBool(c *bool, b *bool) *bool {
	if c == nil && b != nil {
		c = new(bool)
		*c = *b
	}
	return c
}

var stoplosss = regexp.MustCompile(`^(\d+)(%?)$`)

func StopLoss(v string) (uint, float64, error) {
	if v == "" || v == "0" || v == "0%" {
		return 0, 0, nil
	}
	if !stoplosss.MatchString(v) {
		return 0, 0, fmt.Errorf("'%s' does not match /%s/", v, stoplosss)
	}

	m := stoplosss.FindStringSubmatch(v) // [v, $1, $2]
	n, err := strconv.Atoi(m[1])         // $1 => int64
	if err != nil {
		return 0, 0, err
	}
	if m[2] == "%" {
		return 0, float64(n), nil // percent stop-loss
	}
	return uint(n), 0, nil // number stop-loss
}

// validFreq validates the freq value for the given config section and returns
// nil if valid, else returns an error.
func validFreq(freq, config string) error {
	if freq == "" {
		return nil
	}
	if freq == "0" {
		return fmt.Errorf("invalid config.%s: 0: must be greater than zero", config)
	}
	d, err := time.ParseDuration(freq)
	if err != nil {
		return fmt.Errorf("invalid config.%s: %s: %s", config, freq, err)
	}
	if d <= 0 {
		return fmt.Errorf("invalid config.%s: %s (%d): value <= 0; must be greater than zero", config, freq, d)
	}
	return nil
}

func LoadConfig(filePath string, cfg Config, required bool) (Config, error) {
	file, err := filepath.Abs(filePath)
	if err != nil {
		return Config{}, err
	}
	Debug("config file: %s (%s)", filePath, file)

	if _, err := os.Stat(file); err != nil {
		if required {
			return Config{}, fmt.Errorf("config file %s does not exist", filePath)
		}
		Debug("config file doesn't exist")
		return cfg, nil
	}

	bytes, err := os.ReadFile(file)
	if err != nil {
		// err includes file name, e.g. "read config file: open <file>: no such file or directory"
		return Config{}, fmt.Errorf("cannot read config file: %s", err)
	}

	if err := yaml.UnmarshalStrict(bytes, &cfg); err != nil {
		return cfg, fmt.Errorf("cannot decode YAML in %s: %s", file, err)
	}

	return cfg, nil
}

func fileExists(filePath string) bool {
	file, err := filepath.Abs(filePath)
	if err != nil {
		return false
	}
	_, err = os.Stat(file)
	return err == nil
}

// Config represents the Blip startup configuration.
type Config struct {
	// Blip server
	API           ConfigAPI           `yaml:"api,omitempty"`
	HTTP          ConfigHTTP          `yaml:"http,omitempty"`
	MonitorLoader ConfigMonitorLoader `yaml:"monitor-loader,omitempty"`
	Sinks         ConfigSinks         `yaml:"sinks,omitempty"`

	// Monitor defaults
	AWS       ConfigAWS              `yaml:"aws,omitempty"`
	Exporter  ConfigExporter         `yaml:"exporter,omitempty"`
	HA        ConfigHighAvailability `yaml:"ha,omitempty"`
	Heartbeat ConfigHeartbeat        `yaml:"heartbeat,omitempty"`
	MySQL     ConfigMySQL            `yaml:"mysql,omitempty"`
	Plans     ConfigPlans            `yaml:"plans,omitempty"`
	Tags      map[string]string      `yaml:"tags,omitempty"`
	TLS       ConfigTLS              `yaml:"tls,omitempty"`

	Monitors []ConfigMonitor `yaml:"monitors,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		API:           DefaultConfigAPI(),
		MonitorLoader: DefaultConfigMonitorLoader(),
		Sinks:         DefaultConfigSinks(),

		AWS:       DefaultConfigAWS(),
		Exporter:  DefaultConfigExporter(),
		HA:        DefaultConfigHA(),
		Heartbeat: DefaultConfigHeartbeat(),
		MySQL:     DefaultConfigMySQL(),
		Plans:     DefaultConfigPlans(),
		TLS:       DefaultConfigTLS(),

		// Default config does not have any monitors (MySQL instances).
		// If the user does not specify any, Blip attempts to auto-detect
		// local MySQL instances.
		Monitors: []ConfigMonitor{},
	}
}

func (c Config) Validate() error {
	// Blip server
	if err := c.API.Validate(); err != nil {
		return err
	}
	if err := c.HTTP.Validate(); err != nil {
		return err
	}
	if err := c.Sinks.Validate(); err != nil {
		return err
	}
	if err := c.MonitorLoader.Validate(); err != nil {
		return err
	}
	// Monitor defaults
	if err := c.AWS.Validate(); err != nil {
		return err
	}
	if err := c.Exporter.Validate(); err != nil {
		return err
	}
	if err := c.HA.Validate(); err != nil {
		return err
	}
	if err := c.Heartbeat.Validate(); err != nil {
		return err
	}
	if err := c.MySQL.Validate(); err != nil {
		return err
	}
	if err := c.Plans.Validate(); err != nil {
		return err
	}
	if err := c.TLS.Validate(); err != nil {
		return err
	}
	return nil
}

// Redacted returns a copy of the config safe for logging. It redacts inline
// passwords without changing the config used to connect to monitored databases.
func (c Config) Redacted() Config {
	redacted := c
	redacted.HTTP = c.HTTP.Redacted()
	redacted.MySQL.Password = redactPassword(redacted.MySQL.Password)
	redacted.Sinks = c.Sinks.redacted()

	// ConfigMonitor and ConfigPlans can refer to each other through
	// ConfigPlans.Monitor. Preserve shared references and cycles while making
	// copies so redaction never changes the live config.
	seen := map[*ConfigMonitor]*ConfigMonitor{}
	if c.Monitors != nil {
		redacted.Monitors = make([]ConfigMonitor, len(c.Monitors))
		for i := range c.Monitors {
			seen[&c.Monitors[i]] = &redacted.Monitors[i]
		}
		for i := range c.Monitors {
			redacted.Monitors[i] = c.Monitors[i].redacted(seen)
		}
	}
	redacted.Plans = c.Plans.redacted(seen)

	return redacted
}

func (c *Config) InterpolateEnvVars() {
	// Blip server
	c.API.InterpolateEnvVars()
	c.HTTP.InterpolateEnvVars()
	c.Sinks.InterpolateEnvVars()
	c.MonitorLoader.InterpolateEnvVars()
	// Monitor defaults
	c.AWS.InterpolateEnvVars()
	c.Exporter.InterpolateEnvVars()
	c.HA.InterpolateEnvVars()
	c.Heartbeat.InterpolateEnvVars()
	c.MySQL.InterpolateEnvVars()
	c.Plans.InterpolateEnvVars()
	c.TLS.InterpolateEnvVars()
	for k, v := range c.Tags {
		c.Tags[k] = interpolateEnv(v)
	}
}

func (c *Config) ApplyDefaults(b Config) {
	c.API.ApplyDefaults(b)
	c.HTTP.ApplyDefaults(b)
	c.MonitorLoader.ApplyDefaults(b)
	// Blip doesn't set defaults for sinks; they're responsible for that
	// when created
}

// ///////////////////////////////////////////////////////////////////////////
// Blip server
// ///////////////////////////////////////////////////////////////////////////

type ConfigAPI struct {
	Bind    string `yaml:"bind"`
	Disable bool   `yaml:"disable,omitempty"`
}

const (
	DEFAULT_API_BIND = "127.0.0.1:7522"
)

func DefaultConfigAPI() ConfigAPI {
	return ConfigAPI{
		Bind: DEFAULT_API_BIND,
	}
}

func (c ConfigAPI) Validate() error {
	// Since API does run and bind until server.Run, we check the bind addr
	// here to catch an invalid bind during server.Boot
	ln, err := net.Listen("tcp", c.Bind)
	if err != nil {
		return fmt.Errorf("api.bind: %s", err)
	}
	ln.Close()
	return nil
}

func (c *ConfigAPI) InterpolateEnvVars() {
	c.Bind = interpolateEnv(c.Bind)
}

func (c *ConfigAPI) ApplyDefaults(b Config) {
	if c.Bind == "" {
		c.Bind = b.API.Bind
	}
}

// --------------------------------------------------------------------------

type ConfigHTTP struct {
	Proxy string `yaml:"proxy,omityempty"`
}

func DefaultConfigHTTP() ConfigHTTP {
	return ConfigHTTP{}
}

func (c ConfigHTTP) Validate() error {
	return nil
}

func (c *ConfigHTTP) InterpolateEnvVars() {
	c.Proxy = interpolateEnv(c.Proxy)
}

// Redacted returns a copy of the HTTP config safe for logging.
func (c ConfigHTTP) Redacted() ConfigHTTP {
	if c.Proxy == "" {
		return c
	}

	proxyURL, err := url.Parse(c.Proxy)
	if err != nil {
		c.Proxy = "..."
		return c
	}
	c.Proxy = proxyURL.Redacted()
	return c
}

func (c *ConfigHTTP) ApplyDefaults(b Config) {
	if c.Proxy == "" {
		c.Proxy = b.HTTP.Proxy
	}
}

// --------------------------------------------------------------------------

type ConfigMonitorLoader struct {
	Files    []string                 `yaml:"files,omitempty"`
	StopLoss string                   `yaml:"stop-loss,omitempty"`
	AWS      ConfigMonitorLoaderAWS   `yaml:"aws,omitempty"`
	Local    ConfigMonitorLoaderLocal `yaml:"local,omitempty"`
}

type ConfigMonitorLoaderAWS struct {
	Regions []string `yaml:"regions,omitempty"`
}

func (c ConfigMonitorLoaderAWS) Automatic() bool {
	for i := range c.Regions {
		if c.Regions[i] == "auto" {
			return true
		}
	}
	return false
}

type ConfigMonitorLoaderLocal struct {
	DisableAuto     bool `yaml:"disable-auto"`
	DisableAutoRoot bool `yaml:"disable-auto-root"`
}

func DefaultConfigMonitorLoader() ConfigMonitorLoader {
	return ConfigMonitorLoader{}
}

func (c ConfigMonitorLoader) Validate() error {
	if _, _, err := StopLoss(c.StopLoss); err != nil {
		return err
	}
	return nil
}

func (c *ConfigMonitorLoader) InterpolateEnvVars() {
	c.StopLoss = interpolateEnv(c.StopLoss)
	for i := range c.Files {
		c.Files[i] = interpolateEnv(c.Files[i])
	}
}

func (c *ConfigMonitorLoader) ApplyDefaults(b Config) {
}

// ///////////////////////////////////////////////////////////////////////////
// Monitor
// ///////////////////////////////////////////////////////////////////////////

type ConfigMonitor struct {
	MonitorId string `yaml:"id"`
	// DatabaseType selects the database-specific connection and metric module.
	// Empty values retain Blip's historical MySQL behavior.
	DatabaseType DatabaseType `yaml:"database-type,omitempty"`

	// Shared connection identity and credentials. Socket and MyCnf are specific
	// to Blip's built-in MySQL connection factory.
	Socket         string `yaml:"socket,omitempty"`
	Hostname       string `yaml:"hostname,omitempty"`
	MyCnf          string `yaml:"mycnf,omitempty"`
	Username       string `yaml:"username,omitempty"`
	Password       string `yaml:"password,omitempty"`
	PasswordFile   string `yaml:"password-file,omitempty"`
	TimeoutConnect string `yaml:"timeout-connect,omitempty"`

	// Tags are passed to each metric sink. Tags inherit from config.tags,
	// but these monitor.tags take precedent (are not overwritten by config.tags).
	Tags map[string]string `yaml:"tags,omitempty"`

	AWS       ConfigAWS              `yaml:"aws,omitempty"`
	Exporter  ConfigExporter         `yaml:"exporter,omitempty"`
	HA        ConfigHighAvailability `yaml:"ha,omitempty"`
	Heartbeat ConfigHeartbeat        `yaml:"heartbeat,omitempty"`
	Plans     ConfigPlans            `yaml:"plans,omitempty"`
	Plan      string                 `yaml:"plan,omitempty"`
	// DatabaseConfig is opaque configuration owned by the external module
	// selected by DatabaseType. MySQL continues to use the historical monitor
	// fields above and rejects this section.
	DatabaseConfig ConfigDatabase `yaml:"database-config,omitempty"`
	Sinks          ConfigSinks    `yaml:"sinks,omitempty"`
	TLS            ConfigTLS      `yaml:"tls,omitempty"`

	Meta map[string]string `yaml:"meta,omitempty"`
}

const (
	DEFAULT_MONITOR_USERNAME        = "blip"
	DEFAULT_MONITOR_TIMEOUT_CONNECT = "10s"
)

func DefaultConfigMonitor() ConfigMonitor {
	return ConfigMonitor{
		Username:       DEFAULT_MONITOR_USERNAME,
		TimeoutConnect: DEFAULT_MONITOR_TIMEOUT_CONNECT,

		Tags: map[string]string{},

		AWS:       DefaultConfigAWS(),
		Exporter:  DefaultConfigExporter(),
		HA:        DefaultConfigHA(),
		Heartbeat: DefaultConfigHeartbeat(),
		Plans:     DefaultConfigPlans(),
		Sinks:     DefaultConfigSinks(),
		TLS:       DefaultConfigTLS(),
	}
}

// EffectiveDatabaseType returns the configured database type. An omitted
// value retains Blip's historical MySQL behavior. Direct environment-variable
// interpolation is resolved before defaults are selected; database type is a
// structural discriminator and does not support monitor-field interpolation.
func (c ConfigMonitor) EffectiveDatabaseType() DatabaseType {
	databaseType := interpolateEnv(string(c.DatabaseType))
	if databaseType == "" {
		return DatabaseTypeMySQL
	}
	return DatabaseType(databaseType)
}

func (c ConfigMonitor) Validate() error {
	databaseType := c.EffectiveDatabaseType()
	if databaseType == DatabaseTypeMySQL {
		if len(c.DatabaseConfig) > 0 {
			return fmt.Errorf("config.monitor.database-config requires an external database type")
		}
		return nil
	}
	if databaseType == DatabaseTypeAny {
		return fmt.Errorf("config.monitor.database-type: %q is reserved for database-neutral collectors", databaseType)
	}
	if !ValidDatabaseType(databaseType) {
		return fmt.Errorf("config.monitor.database-type: invalid database type %q", databaseType)
	}
	if c.Socket != "" {
		return fmt.Errorf("config.monitor.socket is only supported for database-type %q", DatabaseTypeMySQL)
	}
	if c.MyCnf != "" {
		return fmt.Errorf("config.monitor.mycnf is only supported for database-type %q", DatabaseTypeMySQL)
	}
	if c.Heartbeat.set() {
		return fmt.Errorf("config.monitor.heartbeat is only supported for database-type %q", DatabaseTypeMySQL)
	}
	if c.Plans.Change.set() {
		return fmt.Errorf("config.monitor.plans.change is only supported for database-type %q", DatabaseTypeMySQL)
	}
	if c.Plans.Table != "" {
		return fmt.Errorf("config.monitor.plans.table is only supported for database-type %q", DatabaseTypeMySQL)
	}
	if c.Exporter.set() {
		return fmt.Errorf("config.monitor.exporter is only supported for database-type %q", DatabaseTypeMySQL)
	}
	module, ok := registeredDatabaseModule(databaseType)
	if !ok {
		return fmt.Errorf("config.monitor.database-type: database module %q is not registered", databaseType)
	}
	if err := module.ValidateConfig(c); err != nil {
		return fmt.Errorf("config.monitor.database-config for %q: %w", databaseType, err)
	}
	return nil
}

// Redacted returns a copy of the monitor config safe for logging.
func (c ConfigMonitor) Redacted() ConfigMonitor {
	return c.redacted(map[*ConfigMonitor]*ConfigMonitor{})
}

func (c ConfigMonitor) redacted(seen map[*ConfigMonitor]*ConfigMonitor) ConfigMonitor {
	c.Password = redactPassword(c.Password)
	// External module configuration is opaque to Blip, so redact the entire
	// section rather than guessing which module-owned fields might be secrets.
	c.DatabaseConfig = nil
	c.Sinks = c.Sinks.redacted()
	c.Plans = c.Plans.redacted(seen)
	return c
}

func (c *ConfigMonitor) ApplyDefaults(b Config) {
	databaseType := c.EffectiveDatabaseType()
	if databaseType == DatabaseTypeMySQL {
		if c.Socket == "" {
			c.Socket = b.MySQL.Socket
		}
		if c.Hostname == "" {
			c.Hostname = b.MySQL.Hostname
		}
		if c.MyCnf == "" && b.MySQL.MyCnf != "" {
			c.MyCnf = b.MySQL.MyCnf
		}
		if c.Username == "" && b.MySQL.Username != "" {
			c.Username = b.MySQL.Username
		}
		if c.Password == "" && b.MySQL.Password != "" {
			c.Password = b.MySQL.Password
		}
		if c.TimeoutConnect == "" && b.MySQL.TimeoutConnect != "" {
			c.TimeoutConnect = b.MySQL.TimeoutConnect
		}
	} else {
		if c.Username == "" {
			c.Username = DEFAULT_MONITOR_USERNAME
		}
		if c.TimeoutConnect == "" {
			c.TimeoutConnect = DEFAULT_MONITOR_TIMEOUT_CONNECT
		}
	}
	if len(b.Tags) > 0 {
		if c.Tags == nil {
			c.Tags = map[string]string{}
		}
		for bk, bv := range b.Tags {
			if _, ok := c.Tags[bk]; ok {
				continue
			}
			c.Tags[bk] = bv
		}
	}
	if c.Sinks == nil {
		c.Sinks = ConfigSinks{}
	}
	c.AWS.ApplyDefaults(b)
	c.HA.ApplyDefaults(b)
	// Exporter emulation, heartbeat writes, and plan state changes are
	// MySQL-specific. Do not inherit their global defaults into external database
	// monitors; explicit monitor values remain intact so Validate can report the
	// unsupported configuration.
	if databaseType == DatabaseTypeMySQL {
		c.Exporter.ApplyDefaults(b)
		c.Heartbeat.ApplyDefaults(b)
	}
	c.Plans.applyDefaults(b, databaseType == DatabaseTypeMySQL)
	c.Sinks.ApplyDefaults(b)
	c.TLS.ApplyDefaults(b)
}

func (c *ConfigMonitor) InterpolateEnvVars() {
	c.MonitorId = interpolateEnv(c.MonitorId)
	c.DatabaseType = DatabaseType(interpolateEnv(string(c.DatabaseType)))
	c.MyCnf = interpolateEnv(c.MyCnf)
	c.Socket = interpolateEnv(c.Socket)
	c.Hostname = interpolateEnv(c.Hostname)
	c.Username = interpolateEnv(c.Username)
	c.Password = interpolateEnv(c.Password)
	c.PasswordFile = interpolateEnv(c.PasswordFile)
	c.TimeoutConnect = interpolateEnv(c.TimeoutConnect)
	for k, v := range c.Tags {
		c.Tags[k] = interpolateEnv(v)
	}
	for k, v := range c.Meta {
		c.Meta[k] = interpolateEnv(v)
	}
	c.AWS.InterpolateEnvVars()
	c.Exporter.InterpolateEnvVars()
	c.HA.InterpolateEnvVars()
	c.Heartbeat.InterpolateEnvVars()
	c.Plans.InterpolateEnvVars()
	c.Plan = interpolateEnv(c.Plan)
	c.DatabaseConfig.interpolate(interpolateEnv)
	c.Sinks.InterpolateEnvVars()
	c.TLS.InterpolateEnvVars()
}

func (c *ConfigMonitor) InterpolateMonitor() {
	c.MonitorId = c.interpolateMon(c.MonitorId)
	c.MyCnf = c.interpolateMon(c.MyCnf)
	c.Socket = c.interpolateMon(c.Socket)
	c.Hostname = c.interpolateMon(c.Hostname)
	c.Username = c.interpolateMon(c.Username)
	c.Password = c.interpolateMon(c.Password)
	c.PasswordFile = c.interpolateMon(c.PasswordFile)
	c.TimeoutConnect = c.interpolateMon(c.TimeoutConnect)
	for k, v := range c.Tags {
		c.Tags[k] = c.interpolateMon(v)
	}
	for k, v := range c.Meta {
		c.Meta[k] = c.interpolateMon(v)
	}
	c.AWS.InterpolateMonitor(c)
	c.Exporter.InterpolateMonitor(c)
	c.HA.InterpolateMonitor(c)
	c.Heartbeat.InterpolateMonitor(c)
	c.Plans.InterpolateMonitor(c)
	c.Plan = c.interpolateMon(c.Plan)
	c.DatabaseConfig.interpolate(c.interpolateMon)
	c.Sinks.InterpolateMonitor(c)
	c.TLS.InterpolateMonitor(c)
}

var monvar = regexp.MustCompile(`%{([\w_-]+)\.([\w_.-]+)}`)

func (c *ConfigMonitor) interpolateMon(v string) string {
	if !strings.Contains(v, "%{monitor.") {
		return v
	}
	return monvar.ReplaceAllStringFunc(v, func(match string) string {
		m := monvar.FindStringSubmatch(match)
		if len(m) != 3 || m[1] != "monitor" {
			return match // strict match only
		}
		if strings.HasPrefix(m[2], "tags.") {
			if c.Tags == nil {
				return ""
			}
			s := strings.SplitN(m[2], ".", 2)
			return c.Tags[s[1]]
		} else if strings.HasPrefix(m[2], "meta.") {
			if c.Meta == nil {
				return ""
			}
			s := strings.SplitN(m[2], ".", 2)
			return c.Meta[s[1]]
		}

		return c.fieldValue(m[2])
	})
}

func (c *ConfigMonitor) fieldValue(f string) string {
	switch strings.ToLower(f) {
	case "monitorid", "monitor-id", "id":
		return c.MonitorId
	case "database-type":
		return string(c.DatabaseType)
	case "mycnf":
		return c.MyCnf
	case "socket":
		return c.Socket
	case "hostname":
		return c.Hostname
	case "username":
		return c.Username
	case "password":
		return c.Password
	case "password-file":
		return c.PasswordFile
	case "timeout-connect":
		return c.TimeoutConnect
	default:
		return ""
	}
}

// --------------------------------------------------------------------------

type ConfigAWS struct {
	IAMAuth           *bool  `yaml:"iam-auth,omitempty"`
	PasswordSecret    string `yaml:"password-secret,omitempty"`
	Region            string `yaml:"region,omitempty"`
	DisableAutoRegion *bool  `yaml:"disable-auto-region,omitempty"`
	DisableAutoTLS    *bool  `yaml:"disable-auto-tls,omitempty"`
}

func DefaultConfigAWS() ConfigAWS {
	return ConfigAWS{}
}

func (c ConfigAWS) Validate() error {
	return nil
}

func (c *ConfigAWS) ApplyDefaults(b Config) {
	if c.PasswordSecret == "" {
		c.PasswordSecret = b.AWS.PasswordSecret
	}
	if c.Region == "" {
		c.Region = b.AWS.Region
	}

	c.IAMAuth = setBool(c.IAMAuth, b.AWS.IAMAuth)
	c.DisableAutoRegion = setBool(c.DisableAutoRegion, b.AWS.DisableAutoRegion)
	c.DisableAutoTLS = setBool(c.DisableAutoTLS, b.AWS.DisableAutoTLS)
}

func (c *ConfigAWS) InterpolateEnvVars() {
	c.PasswordSecret = interpolateEnv(c.PasswordSecret)
	c.Region = interpolateEnv(c.Region)
}

func (c *ConfigAWS) InterpolateMonitor(m *ConfigMonitor) {
	c.PasswordSecret = m.interpolateMon(c.PasswordSecret)
	c.Region = m.interpolateMon(c.Region)
}

// --------------------------------------------------------------------------

const (
	EXPORTER_MODE_DUAL   = "dual"   // Blip and exporter run together
	EXPORTER_MODE_LEGACY = "legacy" // only exporter runs

	DEFAULT_EXPORTER_LISTEN_ADDR = "127.0.0.1:9104"
	DEFAULT_EXPORTER_PATH        = "/metrics"
	DEFAULT_EXPORTER_PLAN        = "default-exporter"
)

type ConfigExporter struct {
	Flags map[string]string `yaml:"flags,omitempty"`
	Mode  string            `yaml:"mode,omitempty"`
	Plan  string            `yaml:"plan,omitempty"`
}

func (c ConfigExporter) set() bool {
	return c.Mode != "" || c.Plan != "" || len(c.Flags) > 0
}

func DefaultConfigExporter() ConfigExporter {
	return ConfigExporter{}
}

func (c ConfigExporter) Validate() error {
	if c.Mode == "" {
		return nil // exporter not enabled; skip the rest
	}
	if c.Mode != EXPORTER_MODE_DUAL && c.Mode != EXPORTER_MODE_LEGACY {
		return fmt.Errorf("invalid config.exporter.mode: %s; valid values: dual, legacy", c.Mode)
	}
	return nil
}

func (c *ConfigExporter) ApplyDefaults(b Config) {
	if c.Mode == "" && b.Exporter.Mode != "" {
		c.Mode = b.Exporter.Mode
	}
	if c.Mode == "" {
		return // exporter not enabled; skip the rest
	}

	if c.Plan == "" && b.Exporter.Plan != "" {
		c.Plan = b.Exporter.Plan
	}
	if c.Plan == "" {
		c.Plan = DEFAULT_EXPORTER_PLAN
	}
	if len(b.Exporter.Flags) > 0 {
		if c.Flags == nil {
			c.Flags = map[string]string{}
		}
		for k, v := range b.Exporter.Flags {
			c.Flags[k] = v
		}
	}
}

func (c *ConfigExporter) InterpolateEnvVars() {
	interpolateEnv(c.Mode)
	interpolateEnv(c.Plan)
	for k := range c.Flags {
		c.Flags[k] = interpolateEnv(c.Flags[k])
	}
}

func (c *ConfigExporter) InterpolateMonitor(m *ConfigMonitor) {
	m.interpolateMon(c.Mode)
	m.interpolateMon(c.Plan)
	for k := range c.Flags {
		c.Flags[k] = m.interpolateMon(c.Flags[k])
	}
}

// --------------------------------------------------------------------------

type ConfigHeartbeat struct {
	Freq     string `yaml:"freq,omitempty"`
	SourceId string `yaml:"source-id,omitempty"`
	Role     string `yaml:"role,omitempty"`
	Table    string `yaml:"table,omitempty"`
}

func (c ConfigHeartbeat) set() bool {
	return c.Freq != "" || c.SourceId != "" || c.Role != "" || c.Table != ""
}

const (
	DEFAULT_HEARTBEAT_TABLE = "blip.heartbeat"
)

func DefaultConfigHeartbeat() ConfigHeartbeat {
	return ConfigHeartbeat{}
}

func (c ConfigHeartbeat) Validate() error {
	if err := validFreq(c.Freq, "heartbeat.freq"); err != nil {
		return err
	}
	if c.Freq == "" && (c.SourceId != "" || c.Role != "" || c.Table != "") {
		return fmt.Errorf("invalid config.heartbeat: freq is not set but other values are set; set freq to enable heartbeat")
	}
	return nil
}

func (c *ConfigHeartbeat) ApplyDefaults(b Config) {
	if c.Freq == "" {
		c.Freq = b.Heartbeat.Freq
	}
	if c.Table == "" {
		c.Table = b.Heartbeat.Table
	}
	if c.SourceId == "" {
		c.SourceId = b.Heartbeat.SourceId
	}
	if c.Role == "" {
		c.Role = b.Heartbeat.Role
	}
	if c.Freq != "" && c.Table == "" {
		c.Table = DEFAULT_HEARTBEAT_TABLE
	}
}

func (c *ConfigHeartbeat) InterpolateEnvVars() {
	c.Freq = interpolateEnv(c.Freq)
	c.SourceId = interpolateEnv(c.SourceId)
	c.Role = interpolateEnv(c.Role)
	c.Table = interpolateEnv(c.Table)
}

func (c *ConfigHeartbeat) InterpolateMonitor(m *ConfigMonitor) {
	c.Freq = m.interpolateMon(c.Freq)
	c.SourceId = m.interpolateMon(c.SourceId)
	c.Role = m.interpolateMon(c.Role)
	c.Table = m.interpolateMon(c.Table)
}

// --------------------------------------------------------------------------

// Not implemented yet; placeholders

type ConfigHighAvailability struct{}

func DefaultConfigHA() ConfigHighAvailability {
	return ConfigHighAvailability{}
}

func (c ConfigHighAvailability) Validate() error {
	return nil
}

func (c *ConfigHighAvailability) ApplyDefaults(b Config) {
}

func (c *ConfigHighAvailability) InterpolateEnvVars() {
}

func (c *ConfigHighAvailability) InterpolateMonitor(m *ConfigMonitor) {
}

// --------------------------------------------------------------------------

// ConfigMySQL are monitor defaults for each MySQL connection.
type ConfigMySQL struct {
	Hostname       string `yaml:"hostname,omitempty"`
	MyCnf          string `yaml:"mycnf,omitempty"`
	Password       string `yaml:"password,omitempty"`
	PasswordFile   string `yaml:"password-file,omitempty"`
	Socket         string `yaml:"socket,omitempty"`
	TimeoutConnect string `yaml:"timeout-connect,omitempty"`
	Username       string `yaml:"username,omitempty"`
}

func DefaultConfigMySQL() ConfigMySQL {
	return ConfigMySQL{
		Username:       DEFAULT_MONITOR_USERNAME,
		TimeoutConnect: DEFAULT_MONITOR_TIMEOUT_CONNECT,
	}
}

func (c ConfigMySQL) Validate() error {
	return nil
}

func (c *ConfigMySQL) ApplyDefaults(b Config) {
	if c.Socket == "" {
		c.Socket = b.MySQL.Socket
	}
	if c.Hostname == "" {
		c.Hostname = b.MySQL.Hostname
	}
	if c.MyCnf == "" {
		c.MyCnf = b.MySQL.MyCnf
	}
	if c.Username == "" {
		c.Username = b.MySQL.Username
	}
	if c.Password == "" {
		c.Password = b.MySQL.Password
	}
	if c.PasswordFile == "" {
		c.PasswordFile = b.MySQL.Password
	}
	if c.TimeoutConnect == "" {
		c.TimeoutConnect = b.MySQL.TimeoutConnect
	}
}

func (c *ConfigMySQL) InterpolateEnvVars() {
	c.MyCnf = interpolateEnv(c.MyCnf)
	c.Username = interpolateEnv(c.Username)
	c.Password = interpolateEnv(c.Password)
	c.PasswordFile = interpolateEnv(c.PasswordFile)
	c.TimeoutConnect = interpolateEnv(c.TimeoutConnect)
}

func (c *ConfigMySQL) InterpolateMonitor(m *ConfigMonitor) {
	c.MyCnf = m.interpolateMon(c.MyCnf)
	c.Username = m.interpolateMon(c.Username)
	c.Password = m.interpolateMon(c.Password)
	c.PasswordFile = m.interpolateMon(c.PasswordFile)
	c.TimeoutConnect = m.interpolateMon(c.TimeoutConnect)
}

func (c ConfigMySQL) Redacted() string {
	c.Password = redactPassword(c.Password)
	return fmt.Sprintf("%+v", c)
}

func redactPassword(password string) string {
	if password == "" {
		return ""
	}
	return "..."
}

// --------------------------------------------------------------------------

type ConfigPlans struct {
	Files               []string         `yaml:"files,omitempty"`
	Table               string           `yaml:"table,omitempty"`
	Monitor             *ConfigMonitor   `yaml:"monitor,omitempty"`
	Change              ConfigPlanChange `yaml:"change,omitempty"`
	DisableDefaultPlans bool             `yaml:"disable-default-plans"`
}

const (
	DEFAULT_PLANS_TABLE = "blip.plans"
)

func DefaultConfigPlans() ConfigPlans {
	return ConfigPlans{}
}

func (c ConfigPlans) Validate() error {
	if c.Table == "" || c.Monitor == nil {
		return nil
	}
	if c.Monitor.EffectiveDatabaseType() != DatabaseTypeMySQL {
		return fmt.Errorf("config.plans.monitor.database-type: config.plans.table is only supported for database-type %q", DatabaseTypeMySQL)
	}
	if err := c.Monitor.Validate(); err != nil {
		return fmt.Errorf("config.plans.monitor: %w", err)
	}
	return nil
}

func (c ConfigPlans) redacted(seen map[*ConfigMonitor]*ConfigMonitor) ConfigPlans {
	if c.Monitor == nil {
		return c
	}
	if redacted, ok := seen[c.Monitor]; ok {
		c.Monitor = redacted
		return c
	}

	redacted := &ConfigMonitor{}
	seen[c.Monitor] = redacted
	*redacted = c.Monitor.redacted(seen)
	c.Monitor = redacted
	return c
}

func (c *ConfigPlans) ApplyDefaults(b Config) {
	c.applyDefaults(b, true)
}

func (c *ConfigPlans) applyDefaults(b Config, applyChange bool) {
	if len(c.Files) == 0 && len(b.Plans.Files) > 0 {
		c.Files = make([]string, len(b.Plans.Files))
		copy(c.Files, b.Plans.Files)
	}
	if applyChange {
		c.Change.ApplyDefaults(b)
	}
}

func (c *ConfigPlans) InterpolateEnvVars() {
	for i := range c.Files {
		c.Files[i] = interpolateEnv(c.Files[i])
	}
	c.Table = interpolateEnv(c.Table)
	c.Change.InterpolateEnvVars()
}

func (c *ConfigPlans) InterpolateMonitor(m *ConfigMonitor) {
	for i := range c.Files {
		c.Files[i] = m.interpolateMon(c.Files[i])
	}
	c.Table = m.interpolateMon(c.Table)
	c.Change.InterpolateMonitor(m)
}

type ConfigPlanChange struct {
	Offline  ConfigStatePlan `yaml:"offline,omitempty"`
	Standby  ConfigStatePlan `yaml:"standby,omitempty"`
	ReadOnly ConfigStatePlan `yaml:"read-only,omitempty"`
	Active   ConfigStatePlan `yaml:"active,omitempty"`
}

type ConfigStatePlan struct {
	After string `yaml:"after,omitempty"`
	Plan  string `yaml:"plan,omitempty"`
}

func (c *ConfigPlanChange) ApplyDefaults(b Config) {
	if c.Offline.After == "" {
		c.Offline.After = b.Plans.Change.Offline.After
	}
	if c.Offline.Plan == "" {
		c.Offline.Plan = b.Plans.Change.Offline.Plan
	}

	if c.Standby.After == "" {
		c.Standby.After = b.Plans.Change.Standby.After
	}
	if c.Standby.Plan == "" {
		c.Standby.Plan = b.Plans.Change.Standby.Plan
	}

	if c.ReadOnly.After == "" {
		c.ReadOnly.After = b.Plans.Change.ReadOnly.After
	}
	if c.ReadOnly.Plan == "" {
		c.ReadOnly.Plan = b.Plans.Change.ReadOnly.Plan
	}

	if c.Active.After == "" {
		c.Active.After = b.Plans.Change.Active.After
	}
	if c.Active.Plan == "" {
		c.Active.Plan = b.Plans.Change.Active.Plan
	}
}

func (c *ConfigPlanChange) InterpolateEnvVars() {
	c.Offline.After = interpolateEnv(c.Offline.After)
	c.Offline.Plan = interpolateEnv(c.Offline.Plan)

	c.Standby.After = interpolateEnv(c.Standby.After)
	c.Standby.Plan = interpolateEnv(c.Standby.Plan)

	c.ReadOnly.After = interpolateEnv(c.ReadOnly.After)
	c.ReadOnly.Plan = interpolateEnv(c.ReadOnly.Plan)

	c.Active.After = interpolateEnv(c.Active.After)
	c.Active.Plan = interpolateEnv(c.Active.Plan)
}

func (c *ConfigPlanChange) InterpolateMonitor(m *ConfigMonitor) {
	c.Offline.Plan = m.interpolateMon(c.Offline.Plan)
	c.Standby.Plan = m.interpolateMon(c.Standby.Plan)
	c.ReadOnly.Plan = m.interpolateMon(c.ReadOnly.Plan)
	c.Active.Plan = m.interpolateMon(c.Active.Plan)
}

func (c ConfigPlanChange) Enabled() bool {
	return c.Offline.Plan != "" ||
		c.Standby.Plan != "" ||
		c.ReadOnly.Plan != "" ||
		c.Active.Plan != ""
}

func (c ConfigPlanChange) set() bool {
	return c.Offline.After != "" || c.Offline.Plan != "" ||
		c.Standby.After != "" || c.Standby.Plan != "" ||
		c.ReadOnly.After != "" || c.ReadOnly.Plan != "" ||
		c.Active.After != "" || c.Active.Plan != ""
}

// --------------------------------------------------------------------------

type ConfigSinks map[string]map[string]string

func (c ConfigSinks) redacted() ConfigSinks {
	if c == nil {
		return nil
	}

	redacted := make(ConfigSinks, len(c))
	for sinkName, options := range c {
		if options == nil {
			redacted[sinkName] = nil
			continue
		}

		redacted[sinkName] = make(map[string]string, len(options))
		for optionName := range options {
			redacted[sinkName][optionName] = "..."
		}
	}
	return redacted
}

func DefaultConfigSinks() ConfigSinks {
	return ConfigSinks{}
}

func (c ConfigSinks) Validate() error {
	return nil
}

func (c ConfigSinks) ApplyDefaults(b Config) {
	for bk, bv := range b.Sinks {
		opts := c[bk]
		if opts != nil {
			continue
		}
		c[bk] = map[string]string{}
		for k, v := range bv {
			c[bk][k] = v
		}
	}
}

func (c ConfigSinks) InterpolateEnvVars() {
	for _, opts := range c {
		for k, v := range opts {
			opts[k] = interpolateEnv(v)
		}
	}
}

func (c ConfigSinks) InterpolateMonitor(m *ConfigMonitor) {
	for _, opts := range c {
		for k, v := range opts {
			opts[k] = m.interpolateMon(v)
		}
	}
}

// --------------------------------------------------------------------------

type ConfigTLS struct {
	CA         string `yaml:"ca,omitempty"`   // ssl-ca
	Cert       string `yaml:"cert,omitempty"` // ssl-cert
	Key        string `yaml:"key,omitempty"`  // ssl-key
	SkipVerify *bool  `yaml:"skip-verify,omitempty"`
	Disable    *bool  `yaml:"disable,omitempty"`

	// ssl-mode from a my.cnf (see dbconn.ParseMyCnf)
	MySQLMode string `yaml:"-"`
}

func DefaultConfigTLS() ConfigTLS {
	return ConfigTLS{}
}

func (c ConfigTLS) Validate() error {
	if True(c.Disable) || (c.Cert == "" && c.Key == "" && c.CA == "") {
		return nil // no TLS
	}

	// Any files specified must exist
	if c.CA != "" && !fileExists(c.CA) {
		return fmt.Errorf("config.tls.ca: %s: file does not exist", c.CA)
	}
	if c.Cert != "" && !fileExists(c.Cert) {
		return fmt.Errorf("config.tls.cert: %s: file does not exist", c.Cert)
	}
	if c.Key != "" && !fileExists(c.Key) {
		return fmt.Errorf("config.tls.key: %s: file does not exist", c.Key)
	}

	// The three valid combination of files:
	if c.CA != "" && (c.Cert == "" && c.Key == "") {
		return nil // ca (only) e.g. Amazon RDS CA
	}
	if c.Cert != "" && c.Key != "" {
		return nil // cert + key (using system CA)
	}
	if c.CA != "" && c.Cert != "" && c.Key != "" {
		return nil // ca + cert + key (private CA)
	}

	if c.Cert == "" && c.Key != "" {
		return fmt.Errorf("config.tls: missing cert (cert and key are mutually dependent)")
	}
	if c.Cert != "" && c.Key == "" {
		return fmt.Errorf("config.tls: missing key (cert and key are mutually dependent)")
	}

	return fmt.Errorf("config.tls: invalid combination of files: %+v; valid combinations are: ca; cert and key; ca, cert, and key", c)
}

func (c *ConfigTLS) ApplyDefaults(b Config) {
	if c.Cert == "" {
		c.Cert = b.TLS.Cert
	}
	if c.Key == "" {
		c.Key = b.TLS.Key
	}
	if c.CA == "" {
		c.CA = b.TLS.CA
	}
	if c.MySQLMode == "" {
		c.MySQLMode = b.TLS.MySQLMode
	}
	c.SkipVerify = setBool(c.SkipVerify, b.TLS.SkipVerify)
	c.Disable = setBool(c.Disable, b.TLS.Disable)
}

func (c *ConfigTLS) InterpolateEnvVars() {
	c.Cert = interpolateEnv(c.Cert)
	c.Key = interpolateEnv(c.Key)
	c.CA = interpolateEnv(c.CA)
}

func (c *ConfigTLS) InterpolateMonitor(m *ConfigMonitor) {
	c.Cert = m.interpolateMon(c.Cert)
	c.Key = m.interpolateMon(c.Key)
	c.CA = m.interpolateMon(c.CA)
}

// Set return true if TLS is not disabled and at least one file is specified.
// If not set, Blip ignores the TLS config. If set, Blip validates, loads, and
// registers the TLS config.
func (c ConfigTLS) Set() bool {
	return !True(c.Disable) && c.MySQLMode != "DISABLED" && (c.CA != "" || c.Cert != "" || c.Key != "")
}

// Create tls.Config from the Blip TLS config settings.
func (c ConfigTLS) LoadTLS(server string) (*tls.Config, error) {
	//  WARNING: ConfigTLS.Valid must be called first!
	Debug("TLS for %s: %+v", server, c)
	if !c.Set() {
		return nil, nil
	}

	// Either ServerName or InsecureSkipVerify is required else Go will
	// return an error saying that. If both are set, Go seems to ignore
	// ServerName.
	tlsConfig := &tls.Config{
		ServerName:         server,
		InsecureSkipVerify: True(c.SkipVerify),
	}

	// Root CA (optional)
	if c.CA != "" {
		caCert, err := os.ReadFile(c.CA)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	// Cert and key
	if c.Cert != "" && c.Key != "" {
		cert, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, err
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
