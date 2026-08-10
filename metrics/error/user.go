// Copyright 2024 Block, Inc.

package error

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cashapp/blip/v2"
)

const (
	TRUNCATE_QUERY_USER = "TRUNCATE TABLE performance_schema.events_errors_summary_by_user_by_error"
	BASE_QUERY_USER     = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER FROM performance_schema.events_errors_summary_by_user_by_error"
	GROUP_BY_USER       = " GROUP BY USER"
)

// ErrorUser collects error summary information for the error.user domain.
// https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html
type ErrorUser struct {
	db *sql.DB
	// --
	options map[string]*errorLevelOptions
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &ErrorUser{}

// NewErrorUser makes a new Table collector,
func NewErrorUser(db *sql.DB) *ErrorUser {
	return &ErrorUser{
		db:      db,
		options: make(map[string]*errorLevelOptions),
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *ErrorUser) Domain() string {
	return DOMAIN + "." + SUB_DOMAIN_USER
}

// Help returns the output for blip --print-domains.
func (t *ErrorUser) Help() blip.CollectorHelp {
	h := help(SUB_DOMAIN_USER)
	h.Groups = append(h.Groups, []blip.CollectorKeyValue{
		{Key: GRP_ERR_USER, Value: "the user for the corresponding error"},
	}...)
	h.Options[OPT_INCLUDE] = blip.CollectorHelpOption{
		Name: OPT_INCLUDE,
		Desc: fmt.Sprintf("Comma-separated list of users to include (overrides option %s)", OPT_EXCLUDE),
	}
	h.Options[OPT_INCLUDE] = blip.CollectorHelpOption{
		Name:    OPT_EXCLUDE,
		Desc:    fmt.Sprintf("Comma-separated list of users to exclude (ignored if %s is set).", OPT_INCLUDE),
		Default: "event_scheduler",
	}

	return h
}

// Prepare prepares the collector for the given plan.
func (t *ErrorUser) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	for _, level := range plan.Levels {
		dom, ok := level.Collect[t.Domain()]
		if !ok {
			continue
		}
		if dom.Options == nil {
			dom.Options = make(map[string]string)
		}

		errOpts, err := prepare(dom, SUB_DOMAIN_USER, BASE_QUERY_USER, GROUP_BY_USER)
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

func (t *ErrorUser) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
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
		user      string
		total     map[string]float64 = make(map[string]float64)
	)

	for rows.Next() {
		if err = rows.Scan(&errors, &errorNum, &errorName, &user); err != nil {
			return nil, err
		}

		m := blip.MetricValue{
			Name:  "raised",
			Type:  o.metricType,
			Group: map[string]string{GRP_ERR_NUMBER: errorNum, GRP_ERR_NAME: errorName, GRP_ERR_USER: user},
			Value: float64(errors),
		}

		metrics = append(metrics, m)

		if _, ok := total[user]; !ok {
			total[user] = 0
		}
		total[user] += float64(errors)
	}

	if o.emitTotal {
		for user, value := range total {
			metrics = append(metrics, blip.MetricValue{
				Name:  "raised",
				Type:  o.metricType,
				Group: map[string]string{GRP_ERR_NUMBER: "", GRP_ERR_NAME: "", GRP_ERR_USER: user},
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

func (t *ErrorUser) truncate(ctx context.Context, levelName string) error {
	o, ok := t.options[levelName]
	if !ok {
		return nil
	}

	return truncate(ctx, t.db, o, TRUNCATE_QUERY_USER)
}
