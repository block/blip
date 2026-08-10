// Copyright 2024 Block, Inc.

package tls

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

const (
	DOMAIN = "tls"
)

// have_ssl is deprecated as of MySQL 8.0.26, so:
// @todo https://dev.mysql.com/doc/refman/8.0/en/performance-schema-tls-channel-status-table.html

// TLS collects metrics for the tls domain.
type TLS struct {
	db *sql.DB
}

var _ blip.Collector = &TLS{}

func NewTLS(db *sql.DB) *TLS {
	return &TLS{
		db: db,
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
				Desc: "True (1) if have_ssl = YES, else false (0)",
			},
		},
	}
}

func (c *TLS) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	// This domain only collects 1 metric (and there are no options),
	// so we don't have to prepare anything per-level, just check that
	// the only metric is specified correctly.
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}
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
	return nil, nil
}

func (c *TLS) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	var haveSSL string
	err := c.db.QueryRowContext(ctx, "SELECT @@have_ssl").Scan(&haveSSL)
	if err != nil {
		return nil, fmt.Errorf("tls.enabled failed: %s", err)
	}
	enabled, _ := sqlutil.Float64(haveSSL) // MySQL string value -> 1 or 0
	metrics := []blip.MetricValue{
		{
			Name:  "enabled",
			Type:  blip.BOOL, // treated as GAUGE by sinks with value 0 or 1
			Value: enabled,
		},
	}
	return metrics, nil
}
