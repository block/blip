// Copyright 2024 Block, Inc.

// Package statusglobal provides the status.global metric domain collector.
package statusglobal

/*
	This metric collector is a development example.
	Comments in slash-star blocks like this one are developer docs--please read.
	Comments after double slashes // are real code and Godoc comments that your
	code should comment, too.
*/

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

/*
Begin the collector with a const block.
First define DOMAIN.
Then define any OPT_* const.
Then define other const as needed.
*/
const (
	DOMAIN = "status.global"

	OPT_ALL = "all"

	// Other const here as needed.
)

// Global collects metrics for the status.global domain.
// The source is SHOW GLOBAL STATUS.
type Global struct {
	db *sql.DB
	// --
	keep map[string]map[string]bool // level => metricName => true
	all  map[string]bool            // level => true (collect all vars)
}

// Verify collector implements blip.Collector interface
var _ blip.Collector = &Global{}

// NewGlobal makes a new Global collector.
func NewGlobal(db *sql.DB) *Global {
	return &Global{
		db:   db,
		keep: map[string]map[string]bool{},
		all:  map[string]bool{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (c *Global) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (c *Global) Help() blip.CollectorHelp {
	/*
		Collector help and options are required and used to validate options
		given in a plan. Therefore, this info isn't merely for print, it's
		also functional: it defines options accepted by the collector and any
		valid value for those options. This is second level plan validation
		done in plan/PlanLoader.ValidatePlans.

		The Blip docs describe domains at a high level, so don't reproduce that here.
		This help is for collector-specific stuff, primarily its options.

		The Description should be a short one-liner with 1 or 2 examples, as shown here.
	*/
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Global status variables like 'Queries' and 'Threads_running'",
		Options: map[string]blip.CollectorHelpOption{
			OPT_ALL: {
				Name:    OPT_ALL,
				Desc:    "Collect all status variables",
				Default: "no",
				Values: map[string]string{
					"yes": "Collect all (safe but wasteful)",
					"no":  "Collect only variables listed in metrics",
				},
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (c *Global) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	/*
		Prepare varies slightly for each collector, but the following LEVEL block
		is standard: iterate the plan levels, skip levels that do not collect
		this domain, then prepare levels that do collect this domain. Remember:
		a plan can collect the same domain at different levels, and options at
		each level can be different, so treat each level uniquely.

		Prepare should do all up-front work so that Collect simply collects
		metrics--no more checks or validations.

		This function is serialized, so don't worry about concurrency here.
	*/
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}

		/*
			Prepare is third level plan validation, which started in
			plan/PlanLoader.ValidatePlans. Prepare can trust that the plan and all
			options are valid (that's what first and second level plan validation
			check). When processing options, you only need to check for valid options,
			as shown below.

			Any other validation is domain/collector-specific and optional.
			There's none in this collector, but the repl collector, for example,
			checks that given metrics are valid, since it only exports a few metrics.
		*/

		// Process collector options at this level
		if all, ok := dom.Options[OPT_ALL]; ok && all == "yes" {
			c.all[level.Name] = true // collect all status vars
		} else {
			// Collect (keep) only the given status vars
			metrics := make(map[string]bool, len(dom.Metrics))
			for i := range dom.Metrics {
				metrics[strings.ToLower(dom.Metrics[i])] = true
			}
			c.keep[level.Name] = metrics
		}
	}

	/*
		Don't worry about error handling, plans, and so on: if there's an error here,
		just return it. Blip will handle the rest.
	*/

	return nil, nil
}

// Collect collects metrics at the given level.
func (c *Global) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	/*
		*** Collect can be called concurrently! ***
		So ensure that it's stateless (best option) _or_ guard concurrent read/write access.
		This code is read-only, so no guards.

		Collect is the fast and critical path: speed and efficiency are vital.
		Don't do any checks, validations, and so forth--that should have been
		done once in Prepare. Here, just get metrics from MySQL and return.

		Do NOT retry or worry about error handling: just return any error.
		Blip will handle it. It might be a transient error that goes away
		a few seconds later on the next collection.
	*/

	// The most classic MySQL metrics query:
	rows, err := c.db.QueryContext(ctx, "SHOW GLOBAL STATUS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metrics := []blip.MetricValue{} // status vars converted to Blip metrics
	filter := !c.all[levelName]     // keep all status vars or only some?

	// Iterate rows from SHOW GLOBAL STATUS, convert and save values
	var (
		val  string
		name string
		ok   bool
	)
	for rows.Next() {
		if err = rows.Scan(&name, &val); err != nil {
			return nil, err
		}

		// Blip metric names are lowercase
		name = strings.ToLower(name)

		// If filtering, ignore metric unless listed in plan metrics
		if filter && !c.keep[levelName][name] {
			continue
		}

		// New Blip metric, presume counter (most are) but then check
		m := blip.MetricValue{
			Name: name,
			Type: blip.CUMULATIVE_COUNTER,
		}
		if gauge[m.Name] {
			m.Type = blip.GAUGE
		}

		// Convert value to float64, which handles several special cases.
		// Safe to ignore error because sqlutil.Float64 is highly tested,
		// so error is almost guaranteed to mean the value is a string.
		m.Value, ok = sqlutil.Float64(val)
		if !ok {
			continue
		}

		metrics = append(metrics, m)
	}

	return metrics, nil
}

// gauge is a list of known gauge metrics in SHOW GLOBAL STATUS.
var gauge = map[string]bool{
	"threads_running":                true,
	"threads_connected":              true,
	"prepared_stmt_count":            true,
	"innodb_buffer_pool_pages_dirty": true,
	"innodb_buffer_pool_pages_free":  true,
	"innodb_buffer_pool_pages_total": true,
	"innodb_row_lock_current_waits":  true,
	"innodb_os_log_pending_writes":   true,
	"max_used_connections":           true,
}
