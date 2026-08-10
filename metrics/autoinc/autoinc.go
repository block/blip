// Copyright 2024 Block, Inc.

package autoinc

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cashapp/blip/v2"
)

const (
	DOMAIN = "autoinc"

	OPT_EXCLUDE = "exclude"
	OPT_INCLUDE = "include"
)

// AutoInc collects auto-increment utilization for the autoinc domain.
// https://dev.mysql.com/doc/refman/8.0/en/sys-schema-auto-increment-columns.html
type AutoInc struct {
	db *sql.DB
	// --
	query  map[string]string
	params map[string][]interface{}
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &AutoInc{}

// NewAutoIncrement makes a new AutoIncrement collector,
func NewAutoInc(db *sql.DB) *AutoInc {
	return &AutoInc{
		db:     db,
		query:  map[string]string{},
		params: map[string][]interface{}{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *AutoInc) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (t *AutoInc) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Auto Increment Utilization",
		Options: map[string]blip.CollectorHelpOption{
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
			{Key: "db", Value: "the database name for the corresponding auto increment value"},
			{Key: "tbl", Value: "the table name for the corresponding auto increment value"},
			{Key: "col", Value: "the column name for the corresponding auto increment value"},
			{Key: "data_type", Value: "the data type of the column"},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "usage",
				Type: blip.GAUGE,
				Desc: "The percentage of the auto increment range used",
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (t *AutoInc) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
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

		q, params, err := AutoIncrementQuery(dom.Options)
		if err != nil {
			return nil, err
		}
		t.query[level.Name] = q
		t.params[level.Name] = params
	}
	return nil, nil
}

func (t *AutoInc) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
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
		metrics    []blip.MetricValue
		dbName     string
		tblName    string
		colName    string
		colType    string
		isUnsigned bool
		autoincVal int64
	)

	for rows.Next() {
		if err = rows.Scan(&dbName, &tblName, &colName, &colType, &isUnsigned, &autoincVal); err != nil {
			return nil, err
		}

		var maxSize uint64
		colType = strings.ToLower(colType)
		switch colType {
		case "tinyint":
			maxSize = 255
		case "smallint":
			maxSize = 65535
		case "mediumint":
			maxSize = 16777215
		case "int":
			maxSize = 4294967295
		case "bigint":
			maxSize = 18446744073709551615
		default:
			// unknown type, skip
			continue
		}

		if !isUnsigned {
			maxSize = maxSize >> 1
		} else {
			colType = colType + " unsigned"
		}

		m := blip.MetricValue{
			Name:  "usage",
			Type:  blip.GAUGE,
			Group: map[string]string{"db": dbName, "tbl": tblName, "col": colName, "data_type": colType},
			Value: float64(autoincVal) / float64(maxSize),
		}
		metrics = append(metrics, m)
	}

	return metrics, err
}
