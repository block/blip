// Copyright 2024 Block, Inc.

package error

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cashapp/blip/v2"
)

const (
	TRUNCATE_QUERY_THREAD = "TRUNCATE TABLE performance_schema.events_errors_summary_by_thread_by_error"
	BASE_QUERY_THREAD     = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, THREAD_ID FROM performance_schema.events_errors_summary_by_thread_by_error"
	GROUP_BY_THREAD       = " GROUP BY THREAD_ID"
)

// ErrorThread collects error summary information for the error.thread domain.
// https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html
type ErrorThread struct {
	db *sql.DB
	// --
	options map[string]*errorLevelOptions
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &ErrorThread{}

// NewErrorThread makes a new Table collector,
func NewErrorThread(db *sql.DB) *ErrorThread {
	return &ErrorThread{
		db:      db,
		options: make(map[string]*errorLevelOptions),
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *ErrorThread) Domain() string {
	return DOMAIN + "." + SUB_DOMAIN_THREAD
}

// Help returns the output for blip --print-domains.
func (t *ErrorThread) Help() blip.CollectorHelp {
	h := help(SUB_DOMAIN_THREAD)
	h.Groups = append(h.Groups, []blip.CollectorKeyValue{
		{Key: GRP_ERR_THREAD, Value: "the thread for the corresponding error"},
	}...)
	h.Options[OPT_INCLUDE] = blip.CollectorHelpOption{
		Name: OPT_INCLUDE,
		Desc: fmt.Sprintf("Comma-separated list of threads to include (overrides option %s)", OPT_EXCLUDE),
	}
	h.Options[OPT_INCLUDE] = blip.CollectorHelpOption{
		Name:    OPT_EXCLUDE,
		Desc:    fmt.Sprintf("Comma-separated list of threads to exclude (ignored if %s is set).", OPT_INCLUDE),
		Default: "event_scheduler",
	}

	return h
}

// Prepare prepares the collector for the given plan.
func (t *ErrorThread) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	for _, level := range plan.Levels {
		dom, ok := level.Collect[t.Domain()]
		if !ok {
			continue
		}
		if dom.Options == nil {
			dom.Options = make(map[string]string)
		}

		errOpts, err := prepare(dom, SUB_DOMAIN_THREAD, BASE_QUERY_THREAD, GROUP_BY_THREAD)
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

func (t *ErrorThread) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
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
		thread    string
		total     map[string]float64 = make(map[string]float64)
	)

	for rows.Next() {
		if err = rows.Scan(&errors, &errorNum, &errorName, &thread); err != nil {
			return nil, err
		}

		m := blip.MetricValue{
			Name:  "raised",
			Type:  o.metricType,
			Group: map[string]string{GRP_ERR_NUMBER: errorNum, GRP_ERR_NAME: errorName, GRP_ERR_THREAD: thread},
			Value: float64(errors),
		}

		metrics = append(metrics, m)

		if _, ok := total[thread]; !ok {
			total[thread] = 0
		}
		total[thread] += float64(errors)
	}

	if o.emitTotal {
		for thread, value := range total {
			metrics = append(metrics, blip.MetricValue{
				Name:  "raised",
				Type:  o.metricType,
				Group: map[string]string{GRP_ERR_NUMBER: "", GRP_ERR_NAME: "", GRP_ERR_THREAD: thread},
				Value: value,
			})
		}
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

func (t *ErrorThread) truncate(ctx context.Context, levelName string) error {
	o, ok := t.options[levelName]
	if !ok {
		return nil
	}

	return truncate(ctx, t.db, o, TRUNCATE_QUERY_THREAD)
}
