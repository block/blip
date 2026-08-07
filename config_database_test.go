// Copyright 2026 Block, Inc.

package blip_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cashapp/blip"
)

type testDatabaseModule struct {
	databaseType blip.DatabaseType
	validate     func(blip.ConfigMonitor) error
}

func (m testDatabaseModule) DatabaseType() blip.DatabaseType {
	return m.databaseType
}

func (m testDatabaseModule) ValidateConfig(cfg blip.ConfigMonitor) error {
	if m.validate != nil {
		return m.validate(cfg)
	}
	return nil
}

func registerTestDatabaseModule(t *testing.T, databaseType blip.DatabaseType, validate func(blip.ConfigMonitor) error) {
	t.Helper()
	if err := blip.RegisterDatabaseModule(testDatabaseModule{databaseType: databaseType, validate: validate}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blip.RemoveDatabaseModule(databaseType) })
}

func TestConfigMonitorDatabaseTypeDefaultsToMySQLWithoutMutation(t *testing.T) {
	monitor := blip.ConfigMonitor{}
	monitor.ApplyDefaults(blip.DefaultConfig())

	if monitor.DatabaseType != "" {
		t.Fatalf("omitted database type mutated to %q", monitor.DatabaseType)
	}
	if monitor.EffectiveDatabaseType() != blip.DatabaseTypeMySQL {
		t.Fatalf("effective database type = %q, expected mysql", monitor.EffectiveDatabaseType())
	}
	if len(monitor.DatabaseConfig) != 0 {
		t.Fatalf("external database config added to MySQL monitor: %#v", monitor.DatabaseConfig)
	}
}

func TestConfigMonitorExternalModuleValidationAndInterpolation(t *testing.T) {
	const databaseType blip.DatabaseType = "test-interpolation"
	t.Setenv("BLIP_TEST_DATABASE_TYPE", string(databaseType))
	t.Setenv("BLIP_TEST_DATABASE", "metrics_database")
	t.Setenv("BLIP_TEST_INCLUDE", "app_*")

	type moduleConfig struct {
		Database        string   `yaml:"database"`
		Include         []string `yaml:"include"`
		ApplicationName string   `yaml:"application-name"`
		Nested          struct {
			Address string `yaml:"address"`
		} `yaml:"nested"`
	}

	var validated moduleConfig
	registerTestDatabaseModule(t, databaseType, func(cfg blip.ConfigMonitor) error {
		if cfg.TimeoutConnect != "7s" {
			return errors.New("monitor defaults were not available to module validation")
		}
		return blip.DecodeDatabaseConfig(cfg.DatabaseConfig, &validated)
	})

	monitor := blip.ConfigMonitor{
		MonitorId:      "external-monitor",
		DatabaseType:   "${BLIP_TEST_DATABASE_TYPE}",
		TimeoutConnect: "7s",
		DatabaseConfig: blip.ConfigDatabase{
			"database":         "${BLIP_TEST_DATABASE}",
			"include":          []interface{}{"${BLIP_TEST_INCLUDE}"},
			"application-name": "%{monitor.id}",
			"nested": map[interface{}]interface{}{
				"address": "%{monitor.hostname}",
			},
		},
		Hostname: "database.example:1234",
	}
	monitor.ApplyDefaults(blip.DefaultConfig())
	monitor.InterpolateEnvVars()
	monitor.InterpolateMonitor()

	if err := monitor.Validate(); err != nil {
		t.Fatal(err)
	}
	if monitor.DatabaseType != databaseType {
		t.Fatalf("database type = %q, expected %q", monitor.DatabaseType, databaseType)
	}
	if validated.Database != "metrics_database" {
		t.Fatalf("database = %q", validated.Database)
	}
	if len(validated.Include) != 1 || validated.Include[0] != "app_*" {
		t.Fatalf("include = %#v", validated.Include)
	}
	if validated.ApplicationName != monitor.MonitorId {
		t.Fatalf("application name = %q", validated.ApplicationName)
	}
	if validated.Nested.Address != monitor.Hostname {
		t.Fatalf("nested address = %q", validated.Nested.Address)
	}
}

func TestConfigMonitorExternalModuleInterpolationPreservesTypedContainers(t *testing.T) {
	const databaseType blip.DatabaseType = "test-typed-interpolation"
	t.Setenv("BLIP_TEST_TYPED_VALUE", "from-environment")

	type namedString string
	type namedMap map[string]namedString
	type namedSlice []namedString
	type namedArray [2]namedString
	type namedPointer *namedString
	type namedConfig blip.ConfigDatabase
	type nestedStruct struct {
		Monitor namedString `yaml:"monitor"`
	}
	type namedStruct struct {
		Environment namedString   `yaml:"environment"`
		Nested      *nestedStruct `yaml:"nested"`
		private     namedString
	}
	type decodedStruct struct {
		Environment string `yaml:"environment"`
		Nested      struct {
			Monitor string `yaml:"monitor"`
		} `yaml:"nested"`
	}

	type moduleConfig struct {
		NestedConfig       map[string]string `yaml:"nested-config"`
		TypedMap           map[string]string `yaml:"typed-map"`
		NamedMap           map[string]string `yaml:"named-map"`
		NamedSlice         []string          `yaml:"named-slice"`
		NamedArray         []string          `yaml:"named-array"`
		NamedStruct        decodedStruct     `yaml:"named-struct"`
		NamedStructPointer decodedStruct     `yaml:"named-struct-pointer"`
		Pointer            string            `yaml:"pointer"`
		NilMap             map[string]string `yaml:"nil-map"`
		NilSlice           []string          `yaml:"nil-slice"`
		NilPointer         *string           `yaml:"nil-pointer"`
	}

	var validated moduleConfig
	registerTestDatabaseModule(t, databaseType, func(cfg blip.ConfigMonitor) error {
		return blip.DecodeDatabaseConfig(cfg.DatabaseConfig, &validated)
	})

	pointer := namedString("%{monitor.id}")
	structValue := namedStruct{
		Environment: "${BLIP_TEST_TYPED_VALUE}",
		Nested:      &nestedStruct{Monitor: "%{monitor.id}"},
		private:     "${BLIP_TEST_TYPED_VALUE}",
	}
	monitor := blip.ConfigMonitor{
		MonitorId:    "typed-monitor",
		DatabaseType: databaseType,
		DatabaseConfig: blip.ConfigDatabase{
			"nested-config": namedConfig{
				"environment": namedString("${BLIP_TEST_TYPED_VALUE}"),
			},
			"typed-map": map[string]string{
				"monitor": "%{monitor.id}",
			},
			"named-map": namedMap{
				"environment": "${BLIP_TEST_TYPED_VALUE}",
			},
			"named-slice":          namedSlice{"${BLIP_TEST_TYPED_VALUE}", "%{monitor.id}"},
			"named-array":          namedArray{"${BLIP_TEST_TYPED_VALUE}", "%{monitor.id}"},
			"named-struct":         structValue,
			"named-struct-pointer": &structValue,
			"pointer":              namedPointer(&pointer),
			"nil-map":              map[string]string(nil),
			"nil-slice":            namedSlice(nil),
			"nil-pointer":          (*namedString)(nil),
		},
	}
	monitor.ApplyDefaults(blip.DefaultConfig())
	monitor.InterpolateEnvVars()
	monitor.InterpolateMonitor()

	if err := monitor.Validate(); err != nil {
		t.Fatal(err)
	}

	nestedConfig, ok := monitor.DatabaseConfig["nested-config"].(namedConfig)
	if !ok {
		t.Fatalf("nested config type = %T, expected namedConfig", monitor.DatabaseConfig["nested-config"])
	}
	if got := nestedConfig["environment"]; got != namedString("from-environment") {
		t.Fatalf("nested environment = %q", got)
	}
	if _, ok := nestedConfig["environment"].(namedString); !ok {
		t.Fatalf("nested environment type = %T, expected namedString", nestedConfig["environment"])
	}

	typedMap, ok := monitor.DatabaseConfig["typed-map"].(map[string]string)
	if !ok || typedMap["monitor"] != monitor.MonitorId {
		t.Fatalf("typed map = %#v (%T)", monitor.DatabaseConfig["typed-map"], monitor.DatabaseConfig["typed-map"])
	}
	namedMapValue, ok := monitor.DatabaseConfig["named-map"].(namedMap)
	if !ok || namedMapValue["environment"] != "from-environment" {
		t.Fatalf("named map = %#v (%T)", monitor.DatabaseConfig["named-map"], monitor.DatabaseConfig["named-map"])
	}
	namedSliceValue, ok := monitor.DatabaseConfig["named-slice"].(namedSlice)
	if !ok || len(namedSliceValue) != 2 || namedSliceValue[0] != "from-environment" || namedSliceValue[1] != namedString(monitor.MonitorId) {
		t.Fatalf("named slice = %#v (%T)", monitor.DatabaseConfig["named-slice"], monitor.DatabaseConfig["named-slice"])
	}
	namedArrayValue, ok := monitor.DatabaseConfig["named-array"].(namedArray)
	if !ok || namedArrayValue[0] != "from-environment" || namedArrayValue[1] != namedString(monitor.MonitorId) {
		t.Fatalf("named array = %#v (%T)", monitor.DatabaseConfig["named-array"], monitor.DatabaseConfig["named-array"])
	}
	namedStructValue, ok := monitor.DatabaseConfig["named-struct"].(namedStruct)
	if !ok || namedStructValue.Environment != "from-environment" || namedStructValue.Nested == nil || namedStructValue.Nested.Monitor != namedString(monitor.MonitorId) {
		t.Fatalf("named struct = %#v (%T)", monitor.DatabaseConfig["named-struct"], monitor.DatabaseConfig["named-struct"])
	}
	if namedStructValue.private != "${BLIP_TEST_TYPED_VALUE}" {
		t.Fatalf("unexported field was interpolated: %q", namedStructValue.private)
	}
	namedStructPointer, ok := monitor.DatabaseConfig["named-struct-pointer"].(*namedStruct)
	if !ok || namedStructPointer == nil || namedStructPointer.Environment != "from-environment" || namedStructPointer.Nested == nil || namedStructPointer.Nested.Monitor != namedString(monitor.MonitorId) {
		t.Fatalf("named struct pointer = %#v (%T)", monitor.DatabaseConfig["named-struct-pointer"], monitor.DatabaseConfig["named-struct-pointer"])
	}
	if namedStructPointer.private != "${BLIP_TEST_TYPED_VALUE}" {
		t.Fatalf("unexported pointer field was interpolated: %q", namedStructPointer.private)
	}
	pointerValue, ok := monitor.DatabaseConfig["pointer"].(namedPointer)
	if !ok || pointerValue == nil || *pointerValue != namedString(monitor.MonitorId) {
		t.Fatalf("pointer = %#v (%T)", monitor.DatabaseConfig["pointer"], monitor.DatabaseConfig["pointer"])
	}
	if value, ok := monitor.DatabaseConfig["nil-map"].(map[string]string); !ok || value != nil {
		t.Fatalf("nil map = %#v (%T)", monitor.DatabaseConfig["nil-map"], monitor.DatabaseConfig["nil-map"])
	}
	if value, ok := monitor.DatabaseConfig["nil-slice"].(namedSlice); !ok || value != nil {
		t.Fatalf("nil slice = %#v (%T)", monitor.DatabaseConfig["nil-slice"], monitor.DatabaseConfig["nil-slice"])
	}
	if value, ok := monitor.DatabaseConfig["nil-pointer"].(*namedString); !ok || value != nil {
		t.Fatalf("nil pointer = %#v (%T)", monitor.DatabaseConfig["nil-pointer"], monitor.DatabaseConfig["nil-pointer"])
	}

	if validated.NestedConfig["environment"] != "from-environment" ||
		validated.TypedMap["monitor"] != monitor.MonitorId ||
		validated.NamedMap["environment"] != "from-environment" ||
		len(validated.NamedSlice) != 2 || validated.NamedSlice[1] != monitor.MonitorId ||
		len(validated.NamedArray) != 2 || validated.NamedArray[1] != monitor.MonitorId ||
		validated.NamedStruct.Environment != "from-environment" || validated.NamedStruct.Nested.Monitor != monitor.MonitorId ||
		validated.NamedStructPointer.Environment != "from-environment" || validated.NamedStructPointer.Nested.Monitor != monitor.MonitorId ||
		validated.Pointer != monitor.MonitorId ||
		len(validated.NilMap) != 0 || len(validated.NilSlice) != 0 || validated.NilPointer != nil {
		t.Fatalf("decoded module config = %#v", validated)
	}
}

func TestRegisterDatabaseModuleValidation(t *testing.T) {
	tests := []struct {
		name         string
		databaseType blip.DatabaseType
		wantError    string
	}{
		{name: "empty", wantError: "invalid database type"},
		{name: "whitespace", databaseType: " test ", wantError: "invalid database type"},
		{name: "uppercase", databaseType: "Test", wantError: "invalid database type"},
		{name: "MySQL", databaseType: blip.DatabaseTypeMySQL, wantError: "built into Blip"},
		{name: "neutral marker", databaseType: blip.DatabaseTypeAny, wantError: "reserved"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := blip.RegisterDatabaseModule(testDatabaseModule{databaseType: tt.databaseType})
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("RegisterDatabaseModule error = %v", err)
			}
		})
	}

	const duplicate blip.DatabaseType = "test-duplicate"
	registerTestDatabaseModule(t, duplicate, nil)
	if err := blip.RegisterDatabaseModule(testDatabaseModule{databaseType: duplicate}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if err := blip.RegisterDatabaseModule(nil); err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("nil registration error = %v", err)
	}
	var nilModule *testDatabaseModule
	if err := blip.RegisterDatabaseModule(nilModule); err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("typed nil registration error = %v", err)
	}
}

func TestConfigMonitorExternalDatabaseValidation(t *testing.T) {
	const databaseType blip.DatabaseType = "test-guards"
	registerTestDatabaseModule(t, databaseType, func(blip.ConfigMonitor) error {
		return errors.New("module validation failed")
	})

	tests := []struct {
		name      string
		monitor   blip.ConfigMonitor
		wantError string
	}{
		{
			name:      "unregistered database type",
			monitor:   blip.ConfigMonitor{DatabaseType: "unregistered"},
			wantError: "is not registered",
		},
		{
			name: "database config on implicit MySQL monitor",
			monitor: blip.ConfigMonitor{
				DatabaseConfig: blip.ConfigDatabase{"database": "example"},
			},
			wantError: "requires an external database type",
		},
		{
			name:      "neutral marker as monitor type",
			monitor:   blip.ConfigMonitor{DatabaseType: blip.DatabaseTypeAny},
			wantError: "reserved",
		},
		{
			name:      "monitor interpolation in database type",
			monitor:   blip.ConfigMonitor{DatabaseType: "%{monitor.meta.engine}", Meta: map[string]string{"engine": string(databaseType)}},
			wantError: "invalid database type",
		},
		{
			name:      "my.cnf on external monitor",
			monitor:   blip.ConfigMonitor{DatabaseType: databaseType, MyCnf: "/etc/blip/my.cnf"},
			wantError: "mycnf is only supported",
		},
		{
			name:      "socket on external monitor",
			monitor:   blip.ConfigMonitor{DatabaseType: databaseType, Socket: "/tmp/database.sock"},
			wantError: "socket is only supported",
		},
		{
			name:      "heartbeat on external monitor",
			monitor:   blip.ConfigMonitor{DatabaseType: databaseType, Heartbeat: blip.ConfigHeartbeat{Freq: "1s"}},
			wantError: "heartbeat is only supported",
		},
		{
			name: "plan changing on external monitor",
			monitor: blip.ConfigMonitor{DatabaseType: databaseType, Plans: blip.ConfigPlans{Change: blip.ConfigPlanChange{
				Active: blip.ConfigStatePlan{Plan: "active"},
			}}},
			wantError: "plans.change is only supported",
		},
		{
			name:      "plan table on external monitor",
			monitor:   blip.ConfigMonitor{DatabaseType: databaseType, Plans: blip.ConfigPlans{Table: "blip.plans"}},
			wantError: "plans.table is only supported",
		},
		{
			name:      "exporter on external monitor",
			monitor:   blip.ConfigMonitor{DatabaseType: databaseType, Exporter: blip.ConfigExporter{Mode: blip.EXPORTER_MODE_DUAL, Plan: "external"}},
			wantError: "exporter is only supported",
		},
		{
			name:      "module validation",
			monitor:   blip.ConfigMonitor{DatabaseType: databaseType},
			wantError: "module validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := tt.monitor
			monitor.InterpolateEnvVars()
			monitor.InterpolateMonitor()
			err := monitor.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("got error %v, expected it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestConfigMonitorDatabaseSpecificDefaults(t *testing.T) {
	const databaseType blip.DatabaseType = "test-defaults"
	registerTestDatabaseModule(t, databaseType, nil)

	defaults := blip.DefaultConfig()
	defaults.MySQL.Hostname = "mysql.example:3306"
	defaults.Exporter = blip.ConfigExporter{Mode: blip.EXPORTER_MODE_DUAL}
	defaults.Heartbeat = blip.ConfigHeartbeat{Freq: "1s", Table: "blip.heartbeat"}
	defaults.Plans = blip.ConfigPlans{
		Files:  []string{"shared.yaml"},
		Change: blip.ConfigPlanChange{Active: blip.ConfigStatePlan{Plan: "active"}},
	}

	external := blip.ConfigMonitor{DatabaseType: databaseType}
	external.ApplyDefaults(defaults)
	if external.Hostname != "" {
		t.Fatalf("external monitor inherited MySQL hostname %q", external.Hostname)
	}
	if external.Exporter.Mode != "" || external.Exporter.Plan != "" || len(external.Exporter.Flags) != 0 {
		t.Fatalf("external monitor inherited exporter defaults: %+v", external.Exporter)
	}
	if external.Heartbeat != (blip.ConfigHeartbeat{}) {
		t.Fatalf("external monitor inherited heartbeat defaults: %+v", external.Heartbeat)
	}
	if external.Plans.Change.Enabled() {
		t.Fatalf("external monitor inherited plan-changing defaults: %+v", external.Plans.Change)
	}
	if got := external.Plans.Files; len(got) != 1 || got[0] != "shared.yaml" {
		t.Fatalf("external monitor plan files = %#v", got)
	}
	if err := external.Validate(); err != nil {
		t.Fatalf("external monitor with shared defaults is invalid: %v", err)
	}

	mysql := blip.ConfigMonitor{}
	mysql.ApplyDefaults(defaults)
	if mysql.Hostname != defaults.MySQL.Hostname {
		t.Fatalf("MySQL hostname = %q", mysql.Hostname)
	}
	if mysql.Exporter.Plan != blip.DEFAULT_EXPORTER_PLAN {
		t.Fatalf("MySQL exporter plan = %q", mysql.Exporter.Plan)
	}
	if mysql.Heartbeat == (blip.ConfigHeartbeat{}) || !mysql.Plans.Change.Enabled() {
		t.Fatal("MySQL monitor did not inherit MySQL-specific defaults")
	}
}

func TestConfigMonitorEnvironmentTypeSelectsDefaultsBeforeInterpolation(t *testing.T) {
	const databaseType blip.DatabaseType = "test-env-defaults"
	registerTestDatabaseModule(t, databaseType, nil)
	t.Setenv("BLIP_TEST_ENV_DATABASE_TYPE", string(databaseType))

	defaults := blip.DefaultConfig()
	defaults.MySQL.Hostname = "mysql.example:3306"
	monitor := blip.ConfigMonitor{DatabaseType: "${BLIP_TEST_ENV_DATABASE_TYPE}"}
	monitor.ApplyDefaults(defaults)
	if monitor.Hostname != "" {
		t.Fatalf("external monitor inherited MySQL hostname %q", monitor.Hostname)
	}
	monitor.InterpolateEnvVars()
	monitor.InterpolateMonitor()
	if monitor.DatabaseType != databaseType {
		t.Fatalf("database type = %q", monitor.DatabaseType)
	}
	if err := monitor.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigPlansRejectsExternalTableMonitor(t *testing.T) {
	plans := blip.ConfigPlans{
		Table:   "blip.plans",
		Monitor: &blip.ConfigMonitor{DatabaseType: "external-without-registration"},
	}
	err := plans.Validate()
	if err == nil || !strings.Contains(err.Error(), "config.plans.table is only supported") {
		t.Fatalf("ConfigPlans.Validate error = %v", err)
	}
}

func TestConfigMonitorRedactsOpaqueDatabaseConfig(t *testing.T) {
	monitor := blip.ConfigMonitor{
		DatabaseType: "external",
		DatabaseConfig: blip.ConfigDatabase{
			"password": "do-not-log",
			"nested":   map[string]interface{}{"token": "also-do-not-log"},
		},
	}
	redacted := monitor.Redacted()
	if redacted.DatabaseConfig != nil {
		t.Fatalf("redacted database config = %#v", redacted.DatabaseConfig)
	}
	if monitor.DatabaseConfig["password"] != "do-not-log" {
		t.Fatal("redaction modified live database config")
	}
}

func TestDecodeDatabaseConfigIsStrict(t *testing.T) {
	type moduleConfig struct {
		Database string `yaml:"database"`
	}

	var decoded moduleConfig
	if err := blip.DecodeDatabaseConfig(blip.ConfigDatabase{"database": "metrics"}, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Database != "metrics" {
		t.Fatalf("decoded database = %q", decoded.Database)
	}
	if err := blip.DecodeDatabaseConfig(blip.ConfigDatabase{"unknown": true}, &decoded); err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("strict decode error = %v", err)
	}
	if err := blip.DecodeDatabaseConfig(nil, nil); err == nil {
		t.Fatal("nil decode output succeeded")
	}
	var nilOutput *moduleConfig
	if err := blip.DecodeDatabaseConfig(nil, nilOutput); err == nil {
		t.Fatal("typed nil decode output succeeded")
	}
}

func TestLoadConfigAcceptsOpaqueModuleShapeForStrictModuleDecode(t *testing.T) {
	const databaseType blip.DatabaseType = "test-yaml"
	registerTestDatabaseModule(t, databaseType, func(cfg blip.ConfigMonitor) error {
		var decoded struct {
			Database string `yaml:"database"`
		}
		return blip.DecodeDatabaseConfig(cfg.DatabaseConfig, &decoded)
	})

	configFile := filepath.Join(t.TempDir(), "blip.yaml")
	contents := `monitors:
  - id: external
    database-type: test-yaml
    database-config:
      database: metrics
      unknown: rejected-by-module
`
	if err := os.WriteFile(configFile, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := blip.LoadConfig(configFile, blip.DefaultConfig(), true)
	if err != nil {
		t.Fatalf("Blip strict YAML decode rejected opaque module config: %v", err)
	}
	monitor := cfg.Monitors[0]
	monitor.ApplyDefaults(cfg)
	monitor.InterpolateEnvVars()
	monitor.InterpolateMonitor()
	if err := monitor.Validate(); err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("module strict validation error = %v", err)
	}
}
