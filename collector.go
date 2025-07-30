// Copyright 2024 Block, Inc.

package blip

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Collector collects metrics for a single metric domain.
type Collector interface {
	// Domain returns the Blip domain name.
	Domain() string

	// Help returns collector descipiton, options, and other usage printed by
	// blip --print-domains. Blip uses this information to validate user-provided
	// values in plans.
	Help() CollectorHelp

	// Prepare prepares a plan for future calls to Collect. The return function
	// is called once when the collector is destroyed; it allows the collector
	// to clean up. If Prepare returns an error, Blip will retry preparing the
	// plan. Therefore, Prepare should not retry on error (for example, if MySQL
	// is not online yet).
	Prepare(ctx context.Context, plan Plan) (func(), error)

	// Collect collects metrics for the previously prepared plan. Collect is only
	// called after Prepare returns nil.
	Collect(ctx context.Context, levelName string) ([]MetricValue, error)
}

// Help represents information about a collector.
type CollectorHelp struct {
	Domain      string
	Description string
	Options     map[string]CollectorHelpOption
	Errors      map[string]CollectorHelpError
	Groups      []CollectorKeyValue
	Meta        []CollectorKeyValue
	Metrics     []CollectorMetric
}

type CollectorHelpOption struct {
	Name    string
	Desc    string            // describes Name
	Default string            // key in Values
	Values  map[string]string // value => description
}

type CollectorHelpError struct {
	Name    string
	Handles string
	Default string
}

type CollectorMetric struct {
	Name string
	Desc string // describes Name
	Type byte
}

type CollectorKeyValue struct {
	Key   string
	Value string
}

// Validate returns nil if all the given options are valid, else it an error.
func (h CollectorHelp) Validate(opts map[string]string) error {
	// No input? No error.
	if len(opts) == 0 {
		return nil
	}

	// At least 1 opt given, so error if the collector has no options
	if len(h.Options) == 0 {
		return fmt.Errorf("collector has no options but %d given", len(h.Options))
	}

	// Check each given key and value
	for givenKey, givenValue := range opts {

		// Error if the given key is not accpeted by collector
		o, ok := h.Options[givenKey]
		if !ok {
			return fmt.Errorf("unknown option: %s (run 'blip --print-domains' to list collectors and options)", givenKey)
		}

		// If the collector option has a list of allowed values,
		// error if the given value isn't one of the allowed values
		if len(o.Values) > 0 {
			allowed := false
			for allowedVal := range o.Values {
				if givenValue == allowedVal {
					allowed = true
					break
				}
			}
			if !allowed {
				return fmt.Errorf("invalid value for option %s: %s (run 'blip --print-domains' to list collectors and options)",
					givenKey, givenValue)
			}
		}
	}

	return nil
}

// CollectorFactoryArgs are provided by Blip to a CollectorFactory when making
// a Collector. The factory must use the args to create the collector.
type CollectorFactoryArgs struct {
	// Config is the full and final monitor config. Most collectors do not need
	// this, but some that collect metrics outside MySQL, like cloud metrics,
	// might need additional monitor config values.
	Config ConfigMonitor

	// DB is the connection to MySQL. It is safe for concurrent use, and it is
	// used concurrently by other parts of a monitor. The Collector must not
	// modify the connection, reconnect, and so forth--only use the connection.
	DB *sql.DB

	// MonitorId is the monitor identifier. The Collector must include
	// this value in all errors, output, and so forth. Everything monitor-related
	// in Blip is keyed on monitor ID.
	MonitorId string

	// Validate is true only when the plan loader is validating collectors.
	// Do not use this field.
	Validate bool
}

// A CollectorFactory makes one or more Collector.
type CollectorFactory interface {
	Make(domain string, args CollectorFactoryArgs) (Collector, error)
}

// ErrMore signals that a collector will return more values. See https://block.github.io/blip/develop/collectors/#long-running.
var ErrMore = errors.New("more metrics")
