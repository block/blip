// Copyright 2026 Block, Inc.

package blip

import (
	"fmt"
	"reflect"
	"regexp"
	"sync"

	"gopkg.in/yaml.v2"
)

// DatabaseType identifies the database engine used by one monitor.
type DatabaseType string

const (
	// DatabaseTypeMySQL is Blip's built-in database type. An omitted monitor
	// database type retains this historical behavior.
	DatabaseTypeMySQL DatabaseType = "mysql"

	// DatabaseTypeAny declares that a collector is database-neutral. It is a
	// collector compatibility marker, not a valid monitor database type.
	DatabaseTypeAny DatabaseType = "*"
)

// ConfigDatabase contains configuration owned by an external database module.
// Blip interpolates string values but otherwise treats this map as opaque. A
// module should use DecodeDatabaseConfig to strictly decode it into a typed
// configuration before applying its defaults and validation.
type ConfigDatabase map[string]interface{}

// DatabaseModule validates configuration for one external database type.
// Modules register before server boot. MySQL is built into Blip and does not
// use this interface.
type DatabaseModule interface {
	DatabaseType() DatabaseType
	ValidateConfig(ConfigMonitor) error
}

var databaseModuleRegistry = struct {
	sync.RWMutex
	modules map[DatabaseType]DatabaseModule
}{
	modules: map[DatabaseType]DatabaseModule{},
}

var validDatabaseType = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)

// ValidDatabaseType reports whether a value is a valid concrete monitor and
// collector database type. DatabaseTypeAny is not a concrete type.
func ValidDatabaseType(databaseType DatabaseType) bool {
	return validDatabaseType.MatchString(string(databaseType))
}

// RegisterDatabaseModule registers one external database module. Registering a
// duplicate type or one of Blip's reserved database types returns an error.
func RegisterDatabaseModule(module DatabaseModule) error {
	if nilInterface(module) {
		return fmt.Errorf("database module is nil")
	}
	databaseType := module.DatabaseType()
	if databaseType == DatabaseTypeMySQL {
		return fmt.Errorf("database type %q is built into Blip", databaseType)
	}
	if databaseType == DatabaseTypeAny {
		return fmt.Errorf("database type %q is reserved for database-neutral collectors", databaseType)
	}
	if !ValidDatabaseType(databaseType) {
		return fmt.Errorf("database module has invalid database type %q", databaseType)
	}

	databaseModuleRegistry.Lock()
	defer databaseModuleRegistry.Unlock()
	if _, ok := databaseModuleRegistry.modules[databaseType]; ok {
		return fmt.Errorf("database module %q already registered", databaseType)
	}
	databaseModuleRegistry.modules[databaseType] = module
	return nil
}

// RemoveDatabaseModule removes an external database module. It supports test
// isolation and registration rollback when a larger module enable operation
// fails after registering its database type.
func RemoveDatabaseModule(databaseType DatabaseType) {
	databaseModuleRegistry.Lock()
	defer databaseModuleRegistry.Unlock()
	delete(databaseModuleRegistry.modules, databaseType)
}

func registeredDatabaseModule(databaseType DatabaseType) (DatabaseModule, bool) {
	databaseModuleRegistry.RLock()
	defer databaseModuleRegistry.RUnlock()
	module, ok := databaseModuleRegistry.modules[databaseType]
	return module, ok
}

// DecodeDatabaseConfig strictly decodes opaque monitor database configuration
// into a module-owned typed value.
func DecodeDatabaseConfig(config ConfigDatabase, out interface{}) error {
	if nilInterface(out) {
		return fmt.Errorf("database config output is nil")
	}
	encoded, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode database config: %w", err)
	}
	if err := yaml.UnmarshalStrict(encoded, out); err != nil {
		return fmt.Errorf("decode database config: %w", err)
	}
	return nil
}

func nilInterface(value interface{}) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (c ConfigDatabase) interpolate(interpolate func(string) string) {
	for key, value := range c {
		c[key] = interpolateDatabaseConfigValue(value, interpolate)
	}
}

func interpolateDatabaseConfigValue(value interface{}, interpolate func(string) string) interface{} {
	switch typed := value.(type) {
	case string:
		return interpolate(typed)
	case []interface{}:
		for i := range typed {
			typed[i] = interpolateDatabaseConfigValue(typed[i], interpolate)
		}
	case []string:
		for i := range typed {
			typed[i] = interpolate(typed[i])
		}
	case map[string]interface{}:
		for key, item := range typed {
			typed[key] = interpolateDatabaseConfigValue(item, interpolate)
		}
	case map[interface{}]interface{}:
		for key, item := range typed {
			typed[key] = interpolateDatabaseConfigValue(item, interpolate)
		}
	}
	return value
}
