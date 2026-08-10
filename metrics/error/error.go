package error

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	DOMAIN = "error"

	SUB_DOMAIN_ACCOUNT = "account"
	SUB_DOMAIN_GLOBAL  = "global"
	SUB_DOMAIN_USER    = "user"
	SUB_DOMAIN_HOST    = "host"
	SUB_DOMAIN_THREAD  = "thread"

	OPT_ALL                 = "all"
	OPT_INCLUDE             = "include"
	OPT_EXCLUDE             = "exclude"
	OPT_TRUNCATE_TABLE      = "truncate-table"
	OPT_TRUNCATE_TIMEOUT    = "truncate-timeout"
	OPT_TRUNCATE_ON_STARTUP = "truncate-on-startup"
	OPT_TOTAL               = "total"

	ERR_TRUNCATE_FAILED = "truncate-timeout"
	LOCKWAIT_QUERY      = "SET @@session.lock_wait_timeout=%d"

	GRP_ERR_NUMBER = "error_number"
	GRP_ERR_NAME   = "error_name"
	GRP_ERR_USER   = "error_user"
	GRP_ERR_HOST   = "error_host"
	GRP_ERR_THREAD = "error_thread"
)

type errorLevelOptions struct {
	query             string
	params            []any
	truncate          bool
	truncateOnStartup bool
	truncateTimeout   time.Duration
	lockWaitQuery     string
	stop              bool
	truncateErrPolicy *errors.TruncateErrorPolicy
	metricType        byte
	emitTotal         bool
}

// Returns a default help message for error collectors
func help(subdomain string) blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN + "." + subdomain,
		Description: fmt.Sprintf("Errors Summary by %s", cases.Title(language.English).String(subdomain)),
		Options: map[string]blip.CollectorHelpOption{
			OPT_ALL: {
				Name:    OPT_ALL,
				Desc:    "Collect all errors",
				Default: "yes",
				Values: map[string]string{
					"yes":     "All errors (ignore metrics list)",
					"no":      "Specified metrics",
					"exclude": "Excludes specified metrics",
				},
			},
			OPT_TOTAL: {
				Name:    OPT_TOTAL,
				Desc:    fmt.Sprintf("Return the total number of errors raised, grouped by %s", subdomain),
				Default: "yes",
				Values: map[string]string{
					"yes":  "Return the total number of errors raised",
					"no":   "Do not return the total number of errors raised",
					"only": "Only return the total number of errors raised",
				},
			},
			OPT_TRUNCATE_TABLE: {
				Name:    OPT_TRUNCATE_TABLE,
				Desc:    "If the source table should be truncated to reset data after each retrieval",
				Default: "no",
				Values: map[string]string{
					"yes": "Truncate source table after each retrieval",
					"no":  "Do not truncate source table after each retrieval",
				},
			},
			OPT_TRUNCATE_TIMEOUT: {
				Name:    OPT_TRUNCATE_TIMEOUT,
				Desc:    "The amount of time to attempt to truncate the source table before timing out",
				Default: "250ms",
			},
			OPT_TRUNCATE_ON_STARTUP: {
				Name:    OPT_TRUNCATE_ON_STARTUP,
				Desc:    "If the source table should be truncated on the start of metric collection. Truncation will use the timeout specified in " + OPT_TRUNCATE_TIMEOUT,
				Default: "yes",
				Values: map[string]string{
					"yes": "Truncate source table on startup",
					"no":  "Do not truncate source table on startup",
				},
			},
		},
		Groups: []blip.CollectorKeyValue{
			{Key: GRP_ERR_NUMBER, Value: "the error number, or an empty string for a total"},
			{Key: GRP_ERR_NAME, Value: "the error name, or an empty string for a total"},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "raised",
				Type: blip.CUMULATIVE_COUNTER,
				Desc: "Raised errors",
			},
		},
		Errors: map[string]blip.CollectorHelpError{
			ERR_TRUNCATE_FAILED: {
				Name:    ERR_TRUNCATE_FAILED,
				Handles: "Truncation failures on error summary tables",
				Default: errors.NewPolicy("").String(),
			},
		},
	}
}

// Prepares the error level options for the given domain and query.
func prepare(dom blip.Domain, subdomain, baseQuery, groupBy string) (*errorLevelOptions, error) {
	if dom.Options == nil {
		dom.Options = make(map[string]string)
	}

	if _, ok := dom.Options[OPT_ALL]; !ok {
		dom.Options[OPT_ALL] = "yes"
	}

	if _, ok := dom.Options[OPT_TOTAL]; !ok {
		dom.Options[OPT_TOTAL] = "yes"
	}

	errOpts, err := ErrorsQuery(dom, baseQuery, groupBy, subdomain)
	if err != nil {
		return nil, err
	}

	if total := strings.ToLower(dom.Options[OPT_TOTAL]); total == "only" || total == "no" {
		errOpts.emitTotal = false
	} else {
		errOpts.emitTotal = true
	}

	if truncate, ok := dom.Options[OPT_TRUNCATE_TABLE]; ok && strings.ToLower(truncate) == "yes" {
		errOpts.truncate = true
		errOpts.metricType = blip.DELTA_COUNTER
	} else {
		errOpts.truncate = false // default
		errOpts.metricType = blip.CUMULATIVE_COUNTER
	}

	if truncateTimeout, ok := dom.Options[OPT_TRUNCATE_TIMEOUT]; ok && errOpts.truncate {
		if duration, err := time.ParseDuration(truncateTimeout); err != nil {
			return nil, fmt.Errorf("invalid truncate duration: %v", err)
		} else {
			errOpts.truncateTimeout = duration
		}
	} else {
		errOpts.truncateTimeout = 250 * time.Millisecond // default
	}

	errOpts.truncateOnStartup = true
	if truncate := strings.ToLower(dom.Options[OPT_TRUNCATE_ON_STARTUP]); truncate == "no" {
		errOpts.truncateOnStartup = false
	}

	if errOpts.truncate || errOpts.truncateOnStartup {
		// Setup our lock wait timeout. It needs to be at least as long
		// as our truncate timeout, but the granularity of the lock wait
		// timeout is seconds, so we round up to the nearest second that is
		// greater than our truncate timeout.
		lockWaitTimeout := math.Ceil(errOpts.truncateTimeout.Seconds())
		if lockWaitTimeout < 1.0 {
			lockWaitTimeout = 1
		}

		errOpts.lockWaitQuery = fmt.Sprintf(LOCKWAIT_QUERY, int64(lockWaitTimeout))
		errOpts.truncateErrPolicy = errors.NewTruncateErrorPolicy(dom.Errors[ERR_TRUNCATE_FAILED])
		blip.Debug("error policy: %s=%s", ERR_TRUNCATE_FAILED, errOpts.truncateErrPolicy.Policy)
	}

	return errOpts, nil
}

// Executes a TRUNCATE TABLE statement on the given database connection.
func truncate(ctx context.Context, db *sql.DB, o *errorLevelOptions, truncateQuery string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Set `lock_wait_timeout` to prevent our query from being blocked for too long
	// due to metadata locking. We treat a failure to set the lock wait timeout
	// the same as a truncate timeout, as not setting creates a risk of having a thread
	// hang for an extended period of time.
	_, err = conn.ExecContext(ctx, o.lockWaitQuery)
	if err != nil {
		return err
	}

	trCtx, cancelFn := context.WithTimeout(ctx, o.truncateTimeout)
	defer cancelFn()
	_, err = conn.ExecContext(trCtx, truncateQuery)
	return err
}
