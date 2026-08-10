// Copyright 2024 Block, Inc.

package stmt

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/cashapp/blip/v2"
)

const (
	DOMAIN = "stmt.current"
	query  = `SELECT TIMER_WAIT FROM performance_schema.events_statements_current 
		WHERE END_EVENT_ID IS NULL 
		AND EVENT_NAME NOT LIKE ('statement/com/Binlog%')`
	OPT_THRESHOLD = "slow-threshold"
)

type currentMetrics struct {
	slowest   bool
	slow      bool
	threshold float64
}

// Stmt collects metrics for the stmt domain.
// The source is performance_schema.events_statements_current.
type Current struct {
	db      *sql.DB
	atLevel map[string]currentMetrics
}

var _ blip.Collector = &Current{}

func NewCurrent(db *sql.DB) *Current {
	return &Current{
		db:      db,
		atLevel: map[string]currentMetrics{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (c *Current) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (c *Current) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Statement metrics",
		Options: map[string]blip.CollectorHelpOption{
			OPT_THRESHOLD: {
				Name:    OPT_THRESHOLD,
				Desc:    "The duration (as a duration string) that a query must be active to be considered slow",
				Default: "1.0s",
			},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "slowest",
				Type: blip.GAUGE,
				Desc: "The duration of the oldest active query in microseconds",
			},
			{
				Name: "slow",
				Type: blip.GAUGE,
				Desc: "The count of active slow queries",
			},
		},
	}
}

func (c *Current) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}

		if len(dom.Metrics) == 0 {
			return nil, fmt.Errorf("no metrics specified, expect at least one collector metric (run 'blip --print-domains' to list collector metrics)")
		}

		var m currentMetrics
		for _, name := range dom.Metrics {
			switch name {
			case "slowest":
				m.slowest = true
			case "slow":
				m.slow = true
			default:
				return nil, fmt.Errorf("invalid collector metric: %s (run 'blip --print-domains' to list collector metrics)", name)
			}
		}

		threshold, ok := dom.Options[OPT_THRESHOLD]
		if !ok {
			threshold = c.Help().Options[OPT_THRESHOLD].Default
		}
		d, err := time.ParseDuration(threshold)
		if err != nil {
			return nil, fmt.Errorf("invalid %s value '%s': %v", OPT_THRESHOLD, threshold, err)
		}
		t := float64(d.Microseconds())
		if t < 1.0 {
			return nil, fmt.Errorf("invalid %s value '%s': must be greater than or equal to 1.0μs", OPT_THRESHOLD, threshold)
		}
		m.threshold = t

		c.atLevel[level.Name] = m
	}

	return nil, nil
}

func (c *Current) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	m, ok := c.atLevel[levelName]
	if !ok {
		return nil, nil
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %s", query, err)
	}
	defer rows.Close()

	var times []float64
	for rows.Next() {
		var time float64
		if err := rows.Scan(&time); err != nil {
			return nil, fmt.Errorf("%s failed: %s", query, err)
		}
		times = append(times, time/1e6) // picoseconds to microseconds
	}

	sort.Float64s(times)

	var values []blip.MetricValue

	if m.slowest {
		var slowest float64
		if len(times) > 0 {
			slowest = times[len(times)-1]
		}

		values = append(values, blip.MetricValue{
			Name:  "slowest",
			Type:  blip.GAUGE,
			Value: slowest,
		})
	}

	if m.slow {
		// Count of statements with duration greater than or equal to threshold
		count := len(times) - sort.SearchFloat64s(times, m.threshold)

		values = append(values, blip.MetricValue{
			Name:  "slow",
			Type:  blip.GAUGE,
			Value: float64(count),
		})
	}

	return values, nil
}
