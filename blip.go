// Copyright 2024 Block, Inc.

// Package blip provides high-level data structs and const for integrating with Blip.
package blip

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const VERSION = "2.1.0"

var SHA = ""

// Metric types.
const (
	UNKNOWN byte = iota
	CUMULATIVE_COUNTER
	DELTA_COUNTER
	GAUGE
	BOOL
	EVENT
)

// Metrics are metrics collected for one plan level, from one monitor.
type Metrics struct {
	Begin     time.Time                // when collection started
	End       time.Time                // when collection completed
	MonitorId string                   // ID of monitor
	Plan      string                   // plan name
	Level     string                   // level name
	Interval  uint                     // interval number
	State     string                   // state of monitor
	Values    map[string][]MetricValue // keyed on domain
}

func (m *Metrics) String() string {
	return fmt.Sprintf("%s: %s/%s/%d (%s): %s to %s (%d)",
		m.MonitorId, m.Plan, m.Level, m.Interval, m.State,
		m.Begin.Round(time.Microsecond), m.End.Round(time.Microsecond), m.End.Sub(m.Begin).Round(time.Microsecond))
}

// MetricValue is one metric and its name, type, value, and tags. Tags are optional;
// the other fields are required and always set. This is the lowest-level data struct:
// a Collector reports metric values, which the monitor.Engine organize into Metrics
// by adding the appropriate metadata.
type MetricValue struct {
	// Name is the domain-specific metric name, like threads_running from the
	// status.global collector. Names are lowercase but otherwise not modified
	// (for example, hyphens and underscores are not changed).
	Name string

	// Value is the value of the metric. String values are not supported.
	// Boolean values are reported as 0 and 1.
	Value float64

	// Type is the metric type: GAUGE, CUMULATIVE_COUNTER, and other const.
	Type byte

	// Group is the set of name-value pairs that determine the group to which
	// the metric value belongs. Only certain domains group metrics.
	Group map[string]string

	// Meta is optional key-value pairs that annotate or describe the metric value.
	Meta map[string]string
}

// Sink sends metrics to an external destination.
type Sink interface {
	// Send sends metrics to the sink. It must respect the context timeout, if any.
	Send(context.Context, *Metrics) error

	// Name returns the sink name (lowercase). It is used for monitor status to
	// report sink errors, if any.
	Name() string
}

// SinkFactory makes a Sink for a monitor.
type SinkFactory interface {
	Make(SinkFactoryArgs) (Sink, error)
}

type SinkFactoryArgs struct {
	SinkName  string            // config.monitor.sinks.name (required)
	MonitorId string            // config.monitor.id (required)
	Options   map[string]string // config.monitor.sinks.name: key-value pairs
	Tags      map[string]string // config.monitor.tags
}

// DbCredentials are database credentials parsed or loaded for a connection.
type DbCredentials struct {
	Username string
	Password string
	TLS      ConfigTLS
}

// PasswordSecretParser maps an AWS Secrets Manager payload to database credentials.
// The DbCredentials argument is pre-populated with config defaults and can be modified
// by the parser. The payload is the raw SecretString or SecretBinary value.
type PasswordSecretParser func(context.Context, ConfigMonitor, []byte, *DbCredentials) error

// DefaultPasswordSecretParser parses the default AWS RDS secret shape:
// "password" is required, and "username" is optional.
func DefaultPasswordSecretParser(_ context.Context, cfg ConfigMonitor, payload []byte, credentials *DbCredentials) error {
	if credentials == nil {
		return fmt.Errorf("credentials destination is nil")
	}
	credentials.Username = cfg.Username

	var secretPayload map[string]interface{}
	if err := json.Unmarshal(payload, &secretPayload); err != nil {
		return fmt.Errorf("cannot decode secret as JSON object: %s", err)
	}
	if secretPayload == nil {
		return fmt.Errorf("secret value is 'null' literal")
	}

	username, ok := secretPayload["username"]
	if ok {
		usernameStr, ok := username.(string)
		if ok {
			credentials.Username = usernameStr
		}
	}

	password, ok := secretPayload["password"]
	if !ok {
		return fmt.Errorf("error retrieving 'password' value of secret")
	}
	passwordStr, ok := password.(string)
	if !ok {
		return fmt.Errorf("invalid type for 'password' value of secret")
	}
	credentials.Password = passwordStr

	return nil
}

// Plugins are function callbacks that override specific functionality of Blip.
// Plugins are optional, but if specified it overrides the built-in functionality.
type Plugins struct {
	// LoadConfig loads the Blip config on startup. It's passed the Blip default
	// config that should be applied like:
	//
	//   mycfg.ApplyDefaults(def.DefaultConfig())
	//
	// mycfg is the custom config loaded by the plugin, and def is the default
	// config passed to the plugin. Alternatively, the plugin can set values in
	// def (without unsetting default values). Without defaults, Blip might not
	// work as expected.
	//
	// Do not call InterpolateEnvVars. Blip calls that after loading the config.
	LoadConfig func(Config) (Config, error)

	// LoadMonitors loads monitors on startup and reloads them on POST /monitors/reload.
	LoadMonitors func(Config) ([]ConfigMonitor, error)

	// LoadPlans loads plans on startup.
	LoadPlans func(ConfigPlans) ([]Plan, error)

	// ModifyDB modifies the *sql.DB connection pool. Use with caution.
	ModifyDB func(*sql.DB, string)

	// ParsePasswordSecret maps an AWS Secrets Manager payload to database credentials.
	// If nil, Blip uses DefaultPasswordSecretParser.
	ParsePasswordSecret PasswordSecretParser

	// StartMonitor allows a monitor to start by returning true. Else the monitor
	// is loaded but not started. This is used to load all monitors but start only
	// certain monitors.
	StartMonitor func(ConfigMonitor) bool

	// TransformMetrics transforms metrics before they are sent to sinks.
	// If it returns an error, all metrics are dropped (not sent). The function
	// is shared and called concurrently by all monitors. Use Metrics.MonitorId
	// to determine the source of the metrics. Metrics are ordered by Metrics.Interval
	// should not be reordered if delta counters are used (doing so will result
	// in negative or incorrect values). Otherwise, the slice of metrics can be modified.
	TransformMetrics func([]*Metrics) error
}

// Factories are interfaces that override certain object creation of Blip.
// Factories are optional, but if specified the override the built-in factories.
type Factories struct {
	AWSConfig  AWSConfigFactory
	DbConn     DbFactory
	HTTPClient HTTPClientFactory
}

// Env is the startup environment: command line args and environment variables.
// This is mostly used for testing to override the defaults.
type Env struct {
	Args []string
	Env  []string
}

type AWS struct {
	Region string
}

type AWSConfigFactory interface {
	Make(AWS, string) (aws.Config, error)
}

type DbFactory interface {
	Make(ConfigMonitor) (*sql.DB, string, error)
}

// DbProvider owns the database connections associated with one monitor.
// Primary returns the connection used by existing Blip subsystems and
// collectors. Close releases the primary connection and any additional
// database-specific resources owned by the provider.
type DbProvider interface {
	Primary() *sql.DB
	Close() error
}

// DbProviderFactory is an optional DbFactory capability for database engines
// that need to own more than one connection pool per monitor. Blip preserves
// the existing DbFactory.Make path for factories that do not implement it.
type DbProviderFactory interface {
	DbFactory
	MakeProvider(ConfigMonitor) (DbProvider, string, error)
}

type HTTPClientFactory interface {
	MakeForSink(sinkName, monitorId string, opts, tags map[string]string) (*http.Client, error)
}

// Monitor states used for plan changing: https://block.github.io/blip/plans/changing/
const (
	STATE_NONE      = ""
	STATE_OFFLINE   = "offline"
	STATE_STANDBY   = "standby"
	STATE_READ_ONLY = "read-only"
	STATE_ACTIVE    = "active"
)

var (
	Debugging = false
	debugLog  = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
)

func Debug(msg string, v ...interface{}) {
	if !Debugging {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	msg = fmt.Sprintf("DEBUG %s:%d %s", path.Base(file), line, msg)
	debugLog.Printf(msg, v...)
}

// True returns true if b is non-nil and true.
// This is convenience function related to *bool files in config structs,
// which is required for knowing when a bool config is explicitly set
// or not. If set, it's not changed; if not, it's set to the default value.
// That makes a good config experience but a less than ideal code experience
// because !*b will panic if b is nil, hence the need for this func.
func True(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func Bool(s string) bool {
	v := strings.ToLower(s)
	return v == "true" || v == "yes" || v == "enable" || v == "enabled"
}

func MonitorId(cfg ConfigMonitor) string {
	switch {
	case cfg.MonitorId != "":
		return cfg.MonitorId
	case cfg.Hostname != "":
		return cfg.Hostname
	case cfg.Socket != "":
		return cfg.Socket
	}
	return ""
}

// SetOrDefault returns a if not empty, else it returns b. This is a convenience
// function to define variables with an explicit value or a DEFAULT_* value.
func SetOrDefault(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

var FormatTime func(time.Time) string = func(t time.Time) string {
	return t.Format(time.RFC3339)
}

// TimeLimit returns d minus p percentage (0.1 = 10%) of time up to max.
// For example, (0.1, 5s, 1s) returns 4.5s: 5000ms - 10% = 4500ms.
// But (0.1, 20s, 1s) returns 29s because 10% of 30s = 3s > 1s max, so the
// buffer is reduced to max. This is used to calculate engine max runtime (EMR)
// and collector max runtime (CMR).
func TimeLimit(p float64, d, max time.Duration) time.Duration {
	ns := float64(d)
	return time.Duration(ns - math.Min(ns*p, float64(max)))
}
