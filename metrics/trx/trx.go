// Copyright 2024 Block, Inc.

package trx

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cashapp/blip/v2"
)

const (
	DOMAIN           = "trx"
	OLDEST_TRX_QUERY = `SELECT COALESCE(UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(MIN(trx_started)), 0) t FROM information_schema.innodb_trx;`
)

type trxMetrics struct {
	queryOldest bool
}

// Trx collects metrics for the event.trx domain.
// The source is information_schema.innodb_trx.
type Trx struct {
	db      *sql.DB
	atLevel map[string]trxMetrics
}

// Verify collector implements blip.Collector interface
var _ blip.Collector = &Trx{}

// NewTrx makes a new Trx collector.
func NewTrx(db *sql.DB) *Trx {
	return &Trx{
		db:      db,
		atLevel: map[string]trxMetrics{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (c *Trx) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (c *Trx) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Transaction metrics",
		Options:     map[string]blip.CollectorHelpOption{},
		Metrics: []blip.CollectorMetric{
			{
				Name: "oldest",
				Type: blip.GAUGE,
				Desc: "The time of oldest transaction in seconds",
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (c *Trx) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}

		if len(dom.Metrics) == 0 {
			return nil, fmt.Errorf("no metrics specified, expect at least one collector metric (run 'blip --print-domains' to list collector metrics)")
		}

		m := trxMetrics{}
		for i := range dom.Metrics {
			switch dom.Metrics[i] {
			case "oldest":
				m.queryOldest = true
			default:
				return nil, fmt.Errorf("invalid collector metric: %s (run 'blip --print-domains' to list collector metrics)", dom.Metrics[i])
			}
		}

		c.atLevel[level.Name] = m
	}

	return nil, nil
}

// Collect collects metrics at the given level.
func (c *Trx) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	rm, ok := c.atLevel[levelName]
	if !ok {
		return nil, nil
	}

	var t float64
	err := c.db.QueryRowContext(ctx, OLDEST_TRX_QUERY).Scan(&t)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %s", OLDEST_TRX_QUERY, err)
	}

	metrics := []blip.MetricValue{}
	if rm.queryOldest {
		m := blip.MetricValue{
			Name:  "oldest",
			Type:  blip.GAUGE,
			Value: t,
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}
