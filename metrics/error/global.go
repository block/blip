// Copyright 2024 Block, Inc.

package error

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cashapp/blip/v2"
)

const (
	TRUNCATE_QUERY_GLOBAL = "TRUNCATE TABLE performance_schema.events_errors_summary_global_by_error"
	BASE_QUERY_GLOBAL     = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME FROM performance_schema.events_errors_summary_global_by_error"
	GROUP_BY_GLOBAL       = ""
)

// ErrorGlobal collects error summary information for the error.global domain.
// https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html
type ErrorGlobal struct {
	db *sql.DB
	// --
	options map[string]*errorLevelOptions
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &ErrorGlobal{}

// NewErrorGlobal makes a new Table collector,
func NewErrorGlobal(db *sql.DB) *ErrorGlobal {
	return &ErrorGlobal{
		db:      db,
		options: make(map[string]*errorLevelOptions),
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *ErrorGlobal) Domain() string {
	return DOMAIN + "." + SUB_DOMAIN_GLOBAL
}

// Help returns the output for blip --print-domains.
func (t *ErrorGlobal) Help() blip.CollectorHelp {
	return help(SUB_DOMAIN_GLOBAL)
}

// Prepare prepares the collector for the given plan.
func (t *ErrorGlobal) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	for _, level := range plan.Levels {
		dom, ok := level.Collect[t.Domain()]
		if !ok {
			continue
		}
		if dom.Options == nil {
			dom.Options = make(map[string]string)
		}

		errOpts, err := prepare(dom, SUB_DOMAIN_GLOBAL, BASE_QUERY_GLOBAL, GROUP_BY_GLOBAL)
		if err != nil {
			return nil, err
		}

		t.options[level.Name] = errOpts

		// Run an initial truncate to clear out any old data
		if errOpts.truncateOnStartup {
			err := t.truncate(ctx, level.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to truncate table for %s on level %s: %v", t.Domain(), level.Name, err)
			}
		}
	}
	return nil, nil
}

func (t *ErrorGlobal) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	o, ok := t.options[levelName]
	if !ok {
		return nil, nil
	}

	rows, err := t.db.QueryContext(ctx, o.query, o.params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		metrics   []blip.MetricValue
		errors    int64
		errorNum  string
		errorName string
		total     float64
	)

	for rows.Next() {
		if err = rows.Scan(&errors, &errorNum, &errorName); err != nil {
			return nil, err
		}

		m := blip.MetricValue{
			Name:  "raised",
			Type:  o.metricType,
			Group: map[string]string{GRP_ERR_NUMBER: errorNum, GRP_ERR_NAME: errorName},
			Value: float64(errors),
		}

		metrics = append(metrics, m)

		total += float64(errors)
	}

	if o.emitTotal {
		metrics = append(metrics, blip.MetricValue{
			Name:  "raised",
			Type:  o.metricType,
			Group: map[string]string{GRP_ERR_NUMBER: "", GRP_ERR_NAME: ""},
			Value: total,
		})
	}

	if o.truncate {
		err = t.truncate(ctx, levelName)
		// Process any errors (or lack thereof) with the TruncateErrorPolicy as there is special handling
		// for the metric values that need to be applied, even if there is not an error. See comments
		// in `TruncateErrorPolicy` for more details.
		return o.truncateErrPolicy.TruncateError(err, &o.stop, metrics)
	}

	return metrics, err
}

func (t *ErrorGlobal) truncate(ctx context.Context, levelName string) error {
	o, ok := t.options[levelName]
	if !ok {
		return nil
	}

	return truncate(ctx, t.db, o, TRUNCATE_QUERY_GLOBAL)
}
