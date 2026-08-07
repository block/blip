// Copyright 2024 Block, Inc.

package innodbbufferpool

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

// Table collects buffer pool data from information_schema.innodb_buffer_pool_stats.
// https://dev.mysql.com/doc/refman/8.4/en/information-schema-innodb-buffer-pool-stats-table.html

const (
	DOMAIN  = "innodb.buffer-pool"
	OPT_ALL = "all"

	BASE_QUERY = "SELECT %s FROM information_schema.innodb_buffer_pool_stats"
)

var (
	columnNames = []string{
		"pool_size",
		"free_buffers",
		"database_pages",
		"old_database_pages",
		"modified_database_pages",
		"pending_decompress",
		"pending_reads",
		"pending_flush_lru",
		"pending_flush_list",
		"pages_made_young",
		"pages_not_made_young",
		"pages_made_young_rate",
		"pages_made_not_young_rate",
		"number_pages_read",
		"number_pages_created",
		"number_pages_written",
		"pages_read_rate",
		"pages_create_rate",
		"pages_written_rate",
		"number_pages_get",
		"hit_rate",
		"young_make_per_thousand_get",
		"not_young_make_per_thousand_get",
		"number_pages_read_ahead",
		"number_read_ahead_evicted",
		"read_ahead_rate",
		"read_ahead_evicted_rate",
		"lru_io_total",
		"lru_io_current",
		"uncompress_total",
		"uncompress_current",
	}

	columnSum map[string]string
)

func init() {
	columnSum = make(map[string]string, len(columnNames))
	for _, name := range columnNames {
		columnSum[name] = fmt.Sprintf("SUM(%s) as %s", name, name)
	}
}

// BufferPoolStats collections from the information_schema.innodb_buffer_pool_stats table.
type BufferPoolStats struct {
	db    *sql.DB
	query map[string]string
}

// Verify collector implements blip.Collector interface.
var _ blip.Collector = &BufferPoolStats{}

// NewTable makes a new Table collector,
func NewBufferPoolStats(db *sql.DB) *BufferPoolStats {
	return &BufferPoolStats{
		db:    db,
		query: map[string]string{},
	}
}

// Domain returns the Blip metric domain name (DOMAIN const).
func (t *BufferPoolStats) Domain() string {
	return DOMAIN
}

// Help returns the output for blip --print-domains.
func (t BufferPoolStats) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Buffer Pool Stats (summed over all pools)",
		Options: map[string]blip.CollectorHelpOption{
			OPT_ALL: {
				Name:    OPT_ALL,
				Desc:    "Collect metrics from all columns in the information_schema.innodb_buffer_pool_stats table.",
				Default: "no",
				Values: map[string]string{
					"yes": "All metrics (ignore metrics list)",
					"no":  "Specified metrics",
				},
			},
		},
	}
}

// Prepare prepares the collector for the given plan.
func (c BufferPoolStats) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected at this level
		}

		sumFields := make([]string, 0, len(dom.Metrics))
		all := strings.ToLower(dom.Options[OPT_ALL])
		switch all {
		case "all":
			for _, column := range columnNames {
				sumFields = append(sumFields, columnSum[column])
			}
		default:
			for _, metric := range dom.Metrics {
				value, ok := columnSum[metric]
				if !ok {
					return nil, fmt.Errorf("invalid metric %q", metric)
				}

				sumFields = append(sumFields, value)
			}
		}

		c.query[level.Name] = fmt.Sprintf("SELECT %s FROM information_schema.innodb_buffer_pool_stats", strings.Join(sumFields, ", "))
		blip.Debug("%s: innodb metrics at %s: %s", plan.MonitorId, level.Name, c.query[level.Name])
	}
	return nil, nil
}

func (t BufferPoolStats) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	o, ok := t.query[levelName]
	if !ok {
		return nil, nil
	}

	results, err := sqlutil.RowToTypedMap[float64](ctx, t.db, o)
	if err != nil {
		return nil, err
	}

	metrics := make([]blip.MetricValue, 0, len(results))
	for name, value := range results {
		m := blip.MetricValue{
			Name:  name,
			Type:  blip.CUMULATIVE_COUNTER,
			Value: value,
		}

		if deltas[name] {
			m.Type = blip.DELTA_COUNTER
		}

		metrics = append(metrics, m)
	}

	return metrics, nil
}

// deltas is a list of known delta metrics in information_schema.innodb_buffer_pool_stats.
var deltas = map[string]bool{
	"pages_made_young_rate":     true,
	"pages_made_not_young_rate": true,
	"pages_read_rate":           true,
	"pages_create_rate":         true,
	"pages_written_rate":        true,
	"read_ahead_rate":           true,
	"read_ahead_evicted_rate":   true,
}
