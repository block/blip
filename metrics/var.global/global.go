// Copyright 2024 Block, Inc.

package varglobal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "var.global"

	OPT_SOURCE = "source"
	OPT_ALL    = "all"

	SOURCE_SELECT = "select"
	SOURCE_PFS    = "pfs"
	SOURCE_SHOW   = "show"
)

// Global collects global system variables for the var.global domain.
type Global struct {
	db *sql.DB
	// --
	metrics  map[string][]string      // keyed on level
	queryIn  map[string]string        // keyed on level
	paramsIn map[string][]interface{} // keyed on level
	sourceIn map[string]string        // keyed on level
}

var _ blip.Collector = &Global{}

func NewGlobal(db *sql.DB) *Global {
	return &Global{
		db:       db,
		metrics:  map[string][]string{},
		queryIn:  make(map[string]string),
		paramsIn: make(map[string][]interface{}),
		sourceIn: make(map[string]string),
	}
}

func (c *Global) Domain() string {
	return DOMAIN
}

func (c *Global) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Global system variables (sysvars) like 'innodb_log_file_size' and 'max_connections'",
		Options: map[string]blip.CollectorHelpOption{
			OPT_SOURCE: {
				Name:    OPT_SOURCE,
				Desc:    "Where to collect sysvars from",
				Default: "auto",
				Values: map[string]string{
					"auto":   "Auto-determine best source",
					"select": "@@GLOBAL.metric_name",
					"pfs":    "performance_schema.global_variables",
					"show":   "SHOW GLOBAL VARIABLES",
				},
			},
			OPT_ALL: {
				Name:    OPT_ALL,
				Desc:    "Collect all sysvars",
				Default: "no",
				Values: map[string]string{
					"yes": "Collect all (CAUTION: there are >500 sysvars)",
					"no":  "Collect only sysvars listed in metrics",
				},
			},
		},
	}
}

// Prepares queries for all levels in the plan that contain the "var.global" domain
func (c *Global) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for levelName, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected in this level
		}
		err := c.prepareLevel(levelName, dom.Metrics, dom.Options)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (c *Global) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	switch c.sourceIn[levelName] {
	case SOURCE_SELECT:
		return c.collectSELECT(ctx, levelName)
	case SOURCE_PFS:
		return c.collectRows(ctx, levelName)
	case SOURCE_SHOW:
		return c.collectRows(ctx, levelName)
	}
	panic(fmt.Sprintf("invalid source in Collect %s", c.sourceIn[levelName]))
}

// //////////////////////////////////////////////////////////////////////////
// Internal methods
// //////////////////////////////////////////////////////////////////////////

// Prepares the query for given level based on it's metrics and source option
func (c *Global) prepareLevel(levelName string, metrics []string, options map[string]string) error {

	// Reset in case because prepareLevel can be called multiple times
	// if the LPA changes the plan
	c.sourceIn[levelName] = ""
	c.queryIn[levelName] = ""
	c.metrics[levelName] = []string{}

	// Save metrics to collect for this level
	c.metrics[levelName] = append(c.metrics[levelName], metrics...)

	// -------------------------------------------------------------------------
	// Manual source
	// -------------------------------------------------------------------------

	// If user specified a method, use only that method, whether it works or not
	if src, ok := options[OPT_SOURCE]; ok {
		if len(src) > 0 && src != "auto" {
			switch src {
			case SOURCE_SELECT:
				return c.prepareSELECT(levelName)
			case SOURCE_PFS:
				return c.preparePFS(levelName)
			case SOURCE_SHOW:
				return c.prepareSHOW(levelName, false)
			default:
				return fmt.Errorf("invalid source: %s; valid values: auto, select, pfs, show", src)
			}
		}
	}

	if all, ok := options[OPT_ALL]; ok && strings.ToLower(all) == "yes" {
		return c.prepareSHOW(levelName, true)
	}

	// -------------------------------------------------------------------------
	// Auto source (default)
	// -------------------------------------------------------------------------
	var err error

	if err = c.prepareSELECT(levelName); err == nil {
		return nil
	}
	if err = c.preparePFS(levelName); err == nil {
		return nil
	}
	if err = c.prepareSHOW(levelName, false); err == nil {
		return nil
	}
	return fmt.Errorf("auto source failed, last error: %s", err)
}

func (c *Global) prepareSELECT(levelName string) error {
	var globalMetrics = make([]string, len(c.metrics[levelName]))

	for i, str := range c.metrics[levelName] {
		globalMetrics[i] = fmt.Sprintf("@@GLOBAL.%s", str)
	}
	globalMetricString := strings.Join(globalMetrics, ", ")

	c.queryIn[levelName] = fmt.Sprintf("SELECT CONCAT_WS(',', %s) v", globalMetricString)
	c.sourceIn[levelName] = SOURCE_SELECT
	c.paramsIn[levelName] = []interface{}{}

	// Try collecting, discard metrics
	_, err := c.collectSELECT(context.TODO(), levelName)
	return err
}

func (c *Global) preparePFS(levelName string) error {
	query := fmt.Sprintf("SELECT variable_name, variable_value from performance_schema.global_variables WHERE variable_name in (%s)",
		sqlutil.PlaceholderList(len(c.metrics[levelName])),
	)
	c.queryIn[levelName] = query
	c.sourceIn[levelName] = SOURCE_PFS
	c.paramsIn[levelName] = sqlutil.ToInterfaceArray(c.metrics[levelName])

	// Try collecting, discard metrics
	_, err := c.collectRows(context.TODO(), levelName)
	return err
}

func (c *Global) prepareSHOW(levelName string, all bool) error {
	var query string
	var params []interface{}
	if all {
		params = []interface{}{}
		query = "SHOW GLOBAL VARIABLES"
	} else {
		params = sqlutil.ToInterfaceArray(c.metrics[levelName])
		query = fmt.Sprintf("SHOW GLOBAL VARIABLES WHERE variable_name in (%s)", sqlutil.PlaceholderList(len(c.metrics[levelName])))
	}

	c.queryIn[levelName] = query
	c.sourceIn[levelName] = SOURCE_SHOW
	c.paramsIn[levelName] = params

	// Try collecting, discard metrics
	_, err := c.collectRows(context.TODO(), levelName)
	return err
}

// --------------------------------------------------------------------------

func (c *Global) collectSELECT(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	rows, err := c.db.QueryContext(ctx, c.queryIn[levelName], c.paramsIn[levelName]...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metrics := make([]blip.MetricValue, len(c.metrics[levelName]))
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}

		values := strings.Split(val, ",")
		for i, metric := range c.metrics[levelName] {
			// Many sysvars are not numbers or convertible to numbers--that's ok.
			// Ignore anything we can't convert, which is industry standard practice.
			f, ok := sqlutil.Float64(values[i])
			if !ok {
				continue
			}
			metrics[i] = blip.MetricValue{
				Name:  metric,
				Value: f,
				Type:  blip.GAUGE,
			}
		}
	}

	return metrics, nil
}

// Since both `show` and `pfs` queries return results in same format (ie; 2 columns, name and value)
// use the same logic for querying and retrieving metrics from the results
func (c *Global) collectRows(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	rows, err := c.db.QueryContext(ctx, c.queryIn[levelName], c.paramsIn[levelName]...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metrics := []blip.MetricValue{}

	var val string
	var ok bool
	for rows.Next() {
		m := blip.MetricValue{Type: blip.GAUGE}

		if err = rows.Scan(&m.Name, &val); err != nil {
			return nil, err
		}

		// Many sysvars are not numbers or convertible to numbers--that's ok.
		// Ignore anything we can't convert, which is industry standard practice.
		m.Value, ok = sqlutil.Float64(val)
		if !ok {
			continue
		}

		metrics = append(metrics, m)
	}

	return metrics, err
}
