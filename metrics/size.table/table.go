// Copyright 2024 Block, Inc.

package sizetable

import (
	"context"
	"database/sql"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "size.table"

	opt_total   = "total"
	OPT_EXCLUDE = "exclude"
	OPT_INCLUDE = "include"
)

// Table collects table sizes for domain size.table.
type Table struct {
	db *sql.DB
	// --
	query  map[string]string
	params map[string][]interface{}
	total  map[string]bool
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &Table{}

// NewTable makes a new Table collector,
func NewTable(db *sql.DB) *Table {
	return &Table{
		db:     db,
		query:  map[string]string{},
		params: map[string][]interface{}{},
		total:  map[string]bool{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *Table) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (t *Table) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Table sizes",
		Options: map[string]blip.CollectorHelpOption{
			opt_total: {
				Name:    opt_total,
				Desc:    "Returns total size of all tables",
				Default: "yes",
				Values: map[string]string{
					"yes": "Includes total size of all tables",
					"no":  "Excludes total size of all tables",
				},
			},
			OPT_INCLUDE: {
				Name: OPT_INCLUDE,
				Desc: "Comma-separated list of database or table names to include (overrides option " + OPT_EXCLUDE + ")",
			},
			OPT_EXCLUDE: {
				Name:    OPT_EXCLUDE,
				Desc:    "Comma-separated list of database or table names to exclude (ignored if " + OPT_INCLUDE + " is set)",
				Default: "mysql.*,information_schema.*,performance_schema.*,sys.*",
			},
		},
		Groups: []blip.CollectorKeyValue{
			{Key: "db", Value: "the database name for the corresponding table size, or empty string for all dbs"},
			{Key: "tbl", Value: "the table name for the corresponding table size, or empty string for all tables"},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "bytes",
				Type: blip.GAUGE,
				Desc: "Table size",
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (t *Table) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected in this level
		}
		if dom.Options == nil {
			dom.Options = make(map[string]string)
		}
		if _, ok := dom.Options[OPT_EXCLUDE]; !ok {
			dom.Options[OPT_EXCLUDE] = "mysql.*,information_schema.*,performance_schema.*,sys.*"
		}

		q, params, err := TableSizeQuery(dom.Options)
		if err != nil {
			return nil, err
		}
		t.query[level.Name] = q
		t.params[level.Name] = params

		if dom.Options[opt_total] == "yes" {
			t.total[level.Name] = true
		}
	}
	return nil, nil
}

func (t *Table) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	q, ok := t.query[levelName]
	if !ok {
		return nil, nil
	}

	rows, err := t.db.QueryContext(ctx, q, t.params[levelName]...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		metrics []blip.MetricValue
		dbName  string
		tblName string
		val     string
	)
	total := float64(0)

	for rows.Next() {
		if err = rows.Scan(&dbName, &tblName, &val); err != nil {
			return nil, err
		}

		m := blip.MetricValue{
			Name:  "bytes",
			Type:  blip.GAUGE,
			Group: map[string]string{"db": dbName, "tbl": tblName},
		}
		var ok bool
		m.Value, ok = sqlutil.Float64(val)
		if !ok {
			continue
		}
		total += m.Value
		metrics = append(metrics, m)
	}

	if t.total[levelName] {
		metrics = append(metrics, blip.MetricValue{
			Name:  "bytes",
			Type:  blip.GAUGE,
			Group: map[string]string{"db": "", "tbl": ""},
			Value: total,
		})
	}

	return metrics, err
}
