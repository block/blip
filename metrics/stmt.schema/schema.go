// Copyright 2026 Block, Inc.

package stmtschema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "stmt.schema"

	OPT_INCLUDE = "include"
	OPT_EXCLUDE = "exclude"

	defaultExclude = "mysql,information_schema,performance_schema,sys"
)

var availableMetrics = []string{
	"count_star",
	"sum_timer_wait",
	"sum_lock_time",
	"sum_errors",
	"sum_warnings",
	"sum_rows_affected",
	"sum_rows_sent",
	"sum_rows_examined",
	"sum_created_tmp_tables",
	"sum_created_tmp_disk_tables",
	"sum_select_scan",
	"sum_select_full_join",
	"sum_no_index_used",
	"sum_no_good_index_used",
}

var defaultMetrics = []string{
	"count_star", "sum_timer_wait", "sum_errors", "sum_rows_examined", "sum_rows_sent",
}

type config struct {
	query   string
	params  []interface{}
	metrics []string
}

// Schema collects cumulative statement counters by the statement's schema.
type Schema struct {
	db      *sql.DB
	atLevel map[string]config
}

var _ blip.Collector = &Schema{}

func NewSchema(db *sql.DB) *Schema {
	return &Schema{db: db, atLevel: map[string]config{}}
}

func (c *Schema) Domain() string { return DOMAIN }

func (c *Schema) Help() blip.CollectorHelp {
	metrics := make([]blip.CollectorMetric, 0, len(availableMetrics))
	for _, name := range availableMetrics {
		metrics = append(metrics, blip.CollectorMetric{Name: name, Type: blip.CUMULATIVE_COUNTER})
	}
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Statement counters grouped by Performance Schema statement schema",
		Options: map[string]blip.CollectorHelpOption{
			OPT_INCLUDE: {Name: OPT_INCLUDE, Desc: "Comma-separated schema names to include"},
			OPT_EXCLUDE: {Name: OPT_EXCLUDE, Desc: "Comma-separated schema names to exclude when include is unset", Default: defaultExclude},
		},
		Groups:  []blip.CollectorKeyValue{{Key: "db", Value: "statement schema name"}},
		Metrics: metrics,
	}
}

func (c *Schema) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue
		}
		query, params, names, err := SummaryQuery(dom.Options, dom.Metrics)
		if err != nil {
			return nil, err
		}
		c.atLevel[level.Name] = config{query: query, params: params, metrics: names}
	}
	return nil, nil
}

func (c *Schema) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	config, ok := c.atLevel[levelName]
	if !ok {
		return nil, nil
	}
	rows, err := c.db.QueryContext(ctx, config.query, config.params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []blip.MetricValue
	for rows.Next() {
		var schema string
		counts := make([]float64, len(config.metrics))
		dest := make([]interface{}, len(counts)+1)
		dest[0] = &schema
		for i := range counts {
			dest[i+1] = &counts[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		for i, name := range config.metrics {
			values = append(values, blip.MetricValue{
				Name: name, Value: counts[i], Type: blip.CUMULATIVE_COUNTER,
				Group: map[string]string{"db": schema},
			})
		}
	}
	return values, rows.Err()
}

// SummaryQuery selects only known additive counters. The digest catch-all row
// has no schema, so it cannot be assigned to a schema-level series.
func SummaryQuery(opts map[string]string, requested []string) (string, []interface{}, []string, error) {
	if len(requested) == 0 {
		requested = defaultMetrics
	}
	allowed := make(map[string]bool, len(availableMetrics))
	for _, metric := range availableMetrics {
		allowed[metric] = true
	}
	names := make([]string, 0, len(requested))
	seen := map[string]bool{}
	for _, metric := range requested {
		name := strings.ToLower(metric)
		if !allowed[name] {
			return "", nil, nil, fmt.Errorf("invalid %s metric: %s", DOMAIN, metric)
		}
		if !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}

	columns := make([]string, 0, len(names)+1)
	columns = append(columns, "SCHEMA_NAME")
	for _, name := range names {
		columns = append(columns, fmt.Sprintf("SUM(%s) AS %s", name, name))
	}
	query := "SELECT " + strings.Join(columns, ", ") +
		" FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME IS NOT NULL AND DIGEST IS NOT NULL"

	var schemas []string
	include := false
	if opts[OPT_INCLUDE] != "" {
		schemas = strings.Split(opts[OPT_INCLUDE], ",")
		include = true
	} else {
		exclude := opts[OPT_EXCLUDE]
		if exclude == "" {
			exclude = defaultExclude
		}
		schemas = strings.Split(exclude, ",")
	}
	params := make([]interface{}, len(schemas))
	for i, schema := range schemas {
		params[i] = strings.TrimSpace(schema)
	}
	if include {
		query += " AND SCHEMA_NAME IN (" + sqlutil.PlaceholderList(len(schemas)) + ")"
	} else {
		query += " AND SCHEMA_NAME NOT IN (" + sqlutil.PlaceholderList(len(schemas)) + ")"
	}
	return query + " GROUP BY SCHEMA_NAME", params, names, nil
}
