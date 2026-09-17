// Copyright 2024 Block, Inc.

package tls

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	myerr "github.com/go-mysql/errors"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "tls"

	tlsChannelStatusQuery = "SELECT VALUE FROM performance_schema.tls_channel_status WHERE CHANNEL = ? AND PROPERTY = ?"
	haveSSLQuery          = "SELECT @@have_ssl"
)

type rowScanner interface {
	Scan(dest ...interface{}) error
}

type queryRowFunc func(context.Context, string, ...interface{}) rowScanner

// TLS collects metrics for the tls domain.
type TLS struct {
	queryRow  queryRowFunc
	query     string
	queryArgs []interface{}
}

var _ blip.Collector = &TLS{}

func NewTLS(db *sql.DB) *TLS {
	return &TLS{
		queryRow: func(ctx context.Context, query string, args ...interface{}) rowScanner {
			return db.QueryRowContext(ctx, query, args...)
		},
	}
}

func (c *TLS) Domain() string {
	return DOMAIN
}

func (c *TLS) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "TLS status",
		Options:     map[string]blip.CollectorHelpOption{},
		Metrics: []blip.CollectorMetric{
			{
				Name: "enabled",
				Type: blip.BOOL,
				Desc: "True (1) if the main MySQL connection interface supports encrypted connections, else false (0)",
			},
		},
	}
}

func (c *TLS) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	// This domain only collects 1 metric (and there are no options),
	// so we don't have to prepare anything per-level, just check that
	// the only metric is specified correctly.
	configured := false
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}
		configured = true
		if len(dom.Metrics) == 0 {
			return nil, fmt.Errorf("metric 'enabled' not specified; metrics to collect must be listed under 'metrics:' for each domain")
		}
		if len(dom.Metrics) > 1 {
			return nil, fmt.Errorf("too many metrics specified (%d); this domain collects only 1 metric: enabled", len(dom.Metrics))
		}
		if dom.Metrics[0] != "enabled" {
			return nil, fmt.Errorf("invalid metric: %s; this domain collects only 1 metric: enabled", dom.Metrics[0])
		}
	}

	if !configured {
		return nil, nil
	}

	// MySQL 8.0.21 added tls_channel_status, and MySQL 8.4 removed
	// @@have_ssl. Probe the replacement table instead of relying on a version
	// string because Blip supports multiple MySQL-compatible distributions.
	var enabled string
	err := c.queryRow(ctx, tlsChannelStatusQuery, "mysql_main", "Enabled").Scan(&enabled)
	if err == nil {
		c.query = tlsChannelStatusQuery
		c.queryArgs = []interface{}{"mysql_main", "Enabled"}
		return nil, nil
	}

	// A disabled Performance Schema returns no row, and older MySQL versions
	// and compatible distributions do not have the table.
	// Also preserve the old privilege behavior on MySQL 8.0: @@have_ssl does
	// not require SELECT on Performance Schema, so it remains a valid fallback
	// when the monitoring user cannot read tls_channel_status.
	tlsChannelStatusErr := err
	if !errors.Is(err, sql.ErrNoRows) {
		switch myerr.MySQLErrorCode(err) {
		case 1142, 1146: // SELECT denied, or table does not exist
			// Try the legacy source below.
		default:
			return nil, fmt.Errorf("cannot read TLS status from performance_schema.tls_channel_status: %w", err)
		}
	}

	err = c.queryRow(ctx, haveSSLQuery).Scan(&enabled)
	if err != nil {
		if myerr.MySQLErrorCode(tlsChannelStatusErr) == 1142 {
			return nil, fmt.Errorf("cannot read TLS status: grant SELECT on performance_schema.tls_channel_status (%v); legacy @@have_ssl is unavailable (%v)", tlsChannelStatusErr, err)
		}
		return nil, fmt.Errorf("cannot read legacy TLS status from @@have_ssl: %w", err)
	}
	c.query = haveSSLQuery
	c.queryArgs = nil

	return nil, nil
}

func (c *TLS) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	if c.query == "" {
		return nil, fmt.Errorf("tls.enabled failed: collector is not prepared")
	}

	var tlsEnabled string
	err := c.queryRow(ctx, c.query, c.queryArgs...).Scan(&tlsEnabled)
	if err != nil {
		return nil, fmt.Errorf("tls.enabled failed: %w", err)
	}
	enabled, ok := sqlutil.Float64(tlsEnabled)
	if !ok {
		return nil, fmt.Errorf("tls.enabled failed: cannot convert TLS status %q to a boolean", tlsEnabled)
	}
	metrics := []blip.MetricValue{
		{
			Name:  "enabled",
			Type:  blip.BOOL, // treated as GAUGE by sinks with value 0 or 1
			Value: enabled,
		},
	}
	return metrics, nil
}
