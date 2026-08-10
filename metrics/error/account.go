// Copyright 2024 Block, Inc.

package error

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cashapp/blip/v2"
)

const (
	TRUNCATE_QUERY_ACCOUNT = "TRUNCATE TABLE performance_schema.events_errors_summary_by_account_by_error"
	BASE_QUERY_ACCOUNT     = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error"
	GROUP_BY_ACCOUNT       = " GROUP BY USER, HOST"
)

// ErrorAccount collects error summary information for the error.account domain.
// https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html
type ErrorAccount struct {
	db *sql.DB
	// --
	options map[string]*errorLevelOptions
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &ErrorAccount{}

// NewErrorAccount makes a new Table collector,
func NewErrorAccount(db *sql.DB) *ErrorAccount {
	return &ErrorAccount{
		db:      db,
		options: make(map[string]*errorLevelOptions),
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *ErrorAccount) Domain() string {
	return DOMAIN + "." + SUB_DOMAIN_ACCOUNT
}

// Help returns the output for blip --print-domains.
func (t *ErrorAccount) Help() blip.CollectorHelp {
	h := help(SUB_DOMAIN_ACCOUNT)
	h.Groups = append(h.Groups, []blip.CollectorKeyValue{
		{Key: GRP_ERR_USER, Value: "the user for the corresponding error"},
		{Key: GRP_ERR_HOST, Value: "the host for the corresponding error"},
	}...)
	h.Options[OPT_INCLUDE] = blip.CollectorHelpOption{
		Name: OPT_INCLUDE,
		Desc: fmt.Sprintf("Comma-separated list of accounts (user@host) to include (overrides option %s)", OPT_EXCLUDE),
	}
	h.Options[OPT_INCLUDE] = blip.CollectorHelpOption{
		Name:    OPT_EXCLUDE,
		Desc:    fmt.Sprintf("Comma-separated list of accounts (user@host) to exclude (ignored if %s is set).", OPT_INCLUDE),
		Default: "event_scheduler",
	}

	return h
}

// Prepare prepares the collector for the given plan.
func (t *ErrorAccount) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	for _, level := range plan.Levels {
		dom, ok := level.Collect[t.Domain()]
		if !ok {
			continue
		}
		if dom.Options == nil {
			dom.Options = make(map[string]string)
		}

		errOpts, err := prepare(dom, SUB_DOMAIN_ACCOUNT, BASE_QUERY_ACCOUNT, GROUP_BY_ACCOUNT)
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

func (t *ErrorAccount) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
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
		host      string
		total     map[account]float64 = make(map[account]float64)
	)

	for rows.Next() {
		if err = rows.Scan(&errors, &errorNum, &errorName, &user, &host); err != nil {
			return nil, err
		}

		m := blip.MetricValue{
			Name:  "raised",
			Type:  o.metricType,
			Group: map[string]string{GRP_ERR_NUMBER: errorNum, GRP_ERR_NAME: errorName, GRP_ERR_USER: user, GRP_ERR_HOST: host},
			Value: float64(errors),
		}

		metrics = append(metrics, m)

		accountKey := account{
			User: user,
			Host: host,
		}
		if _, ok := total[accountKey]; !ok {
			total[accountKey] = 0
		}
		total[accountKey] += float64(errors)
	}

	if o.emitTotal {
		for account, value := range total {
			metrics = append(metrics, blip.MetricValue{
				Name:  "raised",
				Type:  o.metricType,
				Group: map[string]string{GRP_ERR_NUMBER: "", GRP_ERR_NAME: "", GRP_ERR_USER: account.User, GRP_ERR_HOST: account.Host},
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

func (t *ErrorAccount) truncate(ctx context.Context, levelName string) error {
	o, ok := t.options[levelName]
	if !ok {
		return nil
	}

	return truncate(ctx, t.db, o, TRUNCATE_QUERY_ACCOUNT)
}

type account struct {
	User string
	Host string
}

func (a account) String() string {
	return fmt.Sprintf("%s@%s", a.User, a.Host)
}
