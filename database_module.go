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
	interpolated := interpolateDatabaseConfigReflect(reflect.ValueOf(value), interpolate)
	if !interpolated.IsValid() {
		return nil
	}
	return interpolated.Interface()
}

func interpolateDatabaseConfigReflect(value reflect.Value, interpolate func(string) string) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		interpolated := interpolateDatabaseConfigReflect(value.Elem(), interpolate)
		result := reflect.New(value.Type()).Elem()
		result.Set(interpolated)
		return result
	case reflect.String:
		result := reflect.New(value.Type()).Elem()
		result.SetString(interpolate(value.String()))
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			result.SetMapIndex(
				iterator.Key(),
				interpolateDatabaseConfigReflect(iterator.Value(), interpolate),
			)
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(interpolateDatabaseConfigReflect(value.Index(i), interpolate))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(interpolateDatabaseConfigReflect(value.Index(i), interpolate))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if !value.Type().Field(i).IsExported() {
				continue
			}
			result.Field(i).Set(interpolateDatabaseConfigReflect(value.Field(i), interpolate))
		}
		return result
	case reflect.Ptr:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(interpolateDatabaseConfigReflect(value.Elem(), interpolate))
		if result.Type() != value.Type() {
			result = result.Convert(value.Type())
		}
		return result
	default:
		return value
	}
}
