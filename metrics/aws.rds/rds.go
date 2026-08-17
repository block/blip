// Copyright 2024 Block, Inc.

package awsrds

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/cashapp/blip/v2"
)

const (
	DOMAIN = "aws.rds"

	OPT_DB_ID = "db-id"
)

type CloudWatchClient interface {
	GetMetricData(ctx context.Context, params *cloudwatch.GetMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
}

func NewCloudWatchClient(awsConfig aws.Config) *cloudwatch.Client {
	return cloudwatch.NewFromConfig(awsConfig)
}

var (
	rdsNamespace = aws.String("AWS/RDS")
	rdsAverage   = aws.String("Average")
	rdsDbId      = aws.String("DBInstanceIdentifier")
	rds60s       = aws.Int32(60)
)

// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MonitoringOverview.html#rds-metrics
// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/monitoring-cloudwatch.html#rds-metrics

// We only collect the average becuase there should only be 1 sample because
// we collect at max resolution for CloudWatch Metrics: 1 minute. We check the
// sample size and warn if there's >1. So whereas the API call is supposed to
// return statitical values, we're actually getting the raw per-minute data points,
// and we let SignalFx do aggregation/stats.

// RDS collects basic RDS metrics: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MonitoringOverview.html#rds-metrics
// Enhanced metrics are not collected yet because they're actually logged to
// CloudWatch Logs, not the CloudWatch Metrics API. So handling that is nontrivial.
type RDS struct {
	client CloudWatchClient
	// --
	dbId      string
	monitorId string
	atLevel   map[string]*cloudwatch.GetMetricDataInput // keyed on level
	latestTs  map[string]map[string]time.Time           // keyed on level => metric
}

func NewRDS(client CloudWatchClient) *RDS {
	return &RDS{
		client:   client,
		atLevel:  map[string]*cloudwatch.GetMetricDataInput{},
		latestTs: map[string]map[string]time.Time{},
	}
}

func (c *RDS) Domain() string {
	return DOMAIN
}

func (c *RDS) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Amazon RDS metrics like 'CPUUtilization' and 'FreeableMemory'",
		Options: map[string]blip.CollectorHelpOption{
			OPT_DB_ID: {
				Name:    OPT_DB_ID,
				Desc:    "Database instance identifier",
				Default: "%%{monitor.id}",
			},
		},
	}
}

func (m *RDS) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {

	m.monitorId = plan.MonitorId

LEVEL:
	for _, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected in this level
		}

		m.dbId = dom.Options[OPT_DB_ID]
		if m.dbId == "" {
			m.dbId = plan.MonitorId
		}

		metrics := make([]types.MetricDataQuery, len(dom.Metrics))
		if len(metrics) == 0 {
			return nil, fmt.Errorf("at %s/%s/aws.rds: no metrics specified; expected at least 1 metric (BinLogDiskUsage, CPUUtilization, etc.)",
				plan.Name, level.Name)
		}

		m.latestTs[level.Name] = map[string]time.Time{}

		for i, metric := range dom.Metrics {
			m.latestTs[level.Name][metric] = time.Time{}

			metrics[i] = types.MetricDataQuery{
				Id: aws.String(strings.ToLower(metric)), // must match /^[a-z][a-zA-Z0-9_]*$/
				MetricStat: &types.MetricStat{
					Stat:   rdsAverage,
					Period: rds60s, // max resolution for CloudWatch Metrics
					Metric: &types.Metric{
						MetricName: aws.String(metric),
						Namespace:  rdsNamespace,
						Dimensions: []types.Dimension{
							{
								Name:  rdsDbId,
								Value: &m.dbId,
							},
						},
					},
				},
			}
		}

		m.atLevel[level.Name] = &cloudwatch.GetMetricDataInput{
			//StartTime: &begin, // set in Collect
			//EndTime:   &now,   // set in Collect
			ScanBy:            types.ScanByTimestampAscending,
			MetricDataQueries: metrics,
		}
	}

	return nil, nil
}

func (m *RDS) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	// Every minute, request the last _2 minutes_ for each metric.
	// This is not a typo, it's because RDS is slow and weird wrt metrics.
	// Most metrics trail by about ~1 min, so we have to trail too, but
	// at least one metric (CPUUtilization) seems always to trail by 2 mins.

	// Since we fetch the last 2 minutes, each metric can return 0, 1, or 2
	// data points. When there's 2 (which is very common), we us only the latest.
	// And to make things extra fun, CPUUtilization usually returns an older
	// timestamp, e.g. other metrics report at 00:01 but CPUUtilization reports
	// at 00:00. This means we have to send two sets of sfx.Metrics per report
	// interval, so this map tracks each unique timestamp.

	input, ok := m.atLevel[levelName]
	if !ok {
		return nil, nil
	}

	now := time.Now()
	begin := now.Add(-2 * time.Minute).Round(time.Minute) // see code comment above

	input.StartTime = &begin
	input.EndTime = &now

	blip.Debug("%s: GetMetricData [%+v]\n", m.monitorId, *input)

	output, err := m.client.GetMetricData(ctx, input)
	if err != nil {
		return nil, err
	}

	metrics := []blip.MetricValue{} // status vars converted to Blip metrics

	for i := range output.MetricDataResults {
		r := output.MetricDataResults[i]
		if len(r.Timestamps) == 0 {
			blip.Debug("%s: zero data points for %s\n", m.monitorId, *r.Label)
		}
		for j := range r.Timestamps {

			metric := *r.Label

			// If AWS ts is not after lastest ts, then it's an old or duplicate value
			// that we've already reported; skip it
			if !r.Timestamps[j].After(m.latestTs[levelName][*r.Label]) {
				blip.Debug("%s: drop: %s %s = %f\n", m.monitorId, r.Timestamps[j], metric, r.Values[j])
				continue
			}
			blip.Debug("%s: keep: %s %s = %f\n", m.monitorId, r.Timestamps[j], metric, r.Values[j])
			m.latestTs[levelName][*r.Label] = r.Timestamps[j]
			m := blip.MetricValue{
				Name:  metric,
				Type:  blip.GAUGE, // almost all RDS metrics are gauges
				Value: r.Values[j],
				Meta: map[string]string{
					"ts": fmt.Sprintf("%d", r.Timestamps[j].UnixMilli()), // must be milliseconds
				},
			}
			// we currently only collect delta counters, if we add any cumulative counters
			// in future we should refactor the following to cater for both
			if isDeltaCounter[metric] {
				m.Type = blip.DELTA_COUNTER
			}
			metrics = append(metrics, m)
		}
	}

	return metrics, nil
}

var isDeltaCounter = map[string]bool{
	"AbortedClients":       true,
	"BacktrackWindowAlert": true,
}

/*
	Example of CPUUtilization trailing by 2 minutes:

  15:25:33.242882 metrics.go:78: [2020-09-30 15:23:33 +0000 UTC] to [2020-09-30 15:25:33 +0000 UTC]
  15:25:33.540432 metrics.go:120:          FreeStorageSpace at 2020-09-30 15:24:00 +0000 UTC
  15:25:33.540470 metrics.go:120:          FreeStorageSpace at 2020-09-30 15:23:00 +0000 UTC
  15:25:33.613576 metrics.go:120:            FreeableMemory at 2020-09-30 15:24:00 +0000 UTC
  15:25:33.613614 metrics.go:120:            FreeableMemory at 2020-09-30 15:23:00 +0000 UTC
  15:25:33.677005 metrics.go:120:           BinLogDiskUsage at 2020-09-30 15:24:00 +0000 UTC
  15:25:33.677040 metrics.go:120:           BinLogDiskUsage at 2020-09-30 15:23:00 +0000 UTC
  15:25:33.738875 metrics.go:120:                  ReadIOPS at 2020-09-30 15:24:00 +0000 UTC
  15:25:33.738914 metrics.go:120:                  ReadIOPS at 2020-09-30 15:23:00 +0000 UTC
  15:25:33.787929 metrics.go:120:                 WriteIOPS at 2020-09-30 15:24:00 +0000 UTC
  15:25:33.787965 metrics.go:120:                 WriteIOPS at 2020-09-30 15:23:00 +0000 UTC
  15:25:34.080282 metrics.go:120:            CPUUtilization at 2020-09-30 15:23:00 +0000 UTC
  15:25:34.131457 metrics.go:115: zero data points for BurstBalance
  15:26:33.242886 metrics.go:78: [2020-09-30 15:24:33 +0000 UTC] to [2020-09-30 15:26:33 +0000 UTC]
  15:26:33.521758 metrics.go:120:          FreeStorageSpace at 2020-09-30 15:25:00 +0000 UTC
  15:26:33.521793 metrics.go:120:          FreeStorageSpace at 2020-09-30 15:24:00 +0000 UTC
  15:26:33.566831 metrics.go:120:            FreeableMemory at 2020-09-30 15:25:00 +0000 UTC
  15:26:33.566871 metrics.go:120:            FreeableMemory at 2020-09-30 15:24:00 +0000 UTC
  15:26:33.618570 metrics.go:120:           BinLogDiskUsage at 2020-09-30 15:25:00 +0000 UTC
  15:26:33.618606 metrics.go:120:           BinLogDiskUsage at 2020-09-30 15:24:00 +0000 UTC
  15:26:33.663773 metrics.go:120:                  ReadIOPS at 2020-09-30 15:25:00 +0000 UTC
  15:26:33.663823 metrics.go:120:                  ReadIOPS at 2020-09-30 15:24:00 +0000 UTC
  15:26:33.754530 metrics.go:120:                 WriteIOPS at 2020-09-30 15:25:00 +0000 UTC
  15:26:33.754565 metrics.go:120:                 WriteIOPS at 2020-09-30 15:24:00 +0000 UTC
  15:26:34.022653 metrics.go:120:            CPUUtilization at 2020-09-30 15:24:00 +0000 UTC
  15:26:34.085970 metrics.go:115: zero data points for BurstBalance
  15:27:33.242881 metrics.go:78: [2020-09-30 15:25:33 +0000 UTC] to [2020-09-30 15:27:33 +0000 UTC]
  15:27:33.494566 metrics.go:120:          FreeStorageSpace at 2020-09-30 15:25:00 +0000 UTC
  14:27:33.494605 metrics.go:120:          FreeStorageSpace at 2020-09-30 15:26:00 +0000 UTC
  15:27:33.555669 metrics.go:120:            FreeableMemory at 2020-09-30 15:25:00 +0000 UTC
  15:27:33.555717 metrics.go:120:            FreeableMemory at 2020-09-30 15:26:00 +0000 UTC
  15:27:33.608138 metrics.go:120:           BinLogDiskUsage at 2020-09-30 15:25:00 +0000 UTC
  15:27:33.608188 metrics.go:120:           BinLogDiskUsage at 2020-09-30 15:26:00 +0000 UTC
  15:27:33.657514 metrics.go:120:                  ReadIOPS at 2020-09-30 15:26:00 +0000 UTC
  15:27:33.657587 metrics.go:120:                  ReadIOPS at 2020-09-30 15:25:00 +0000 UTC
  15:27:33.706865 metrics.go:120:                 WriteIOPS at 2020-09-30 15:26:00 +0000 UTC
  15:27:33.706901 metrics.go:120:                 WriteIOPS at 2020-09-30 15:25:00 +0000 UTC
  15:27:33.934436 metrics.go:120:            CPUUtilization at 2020-09-30 15:25:00 +0000 UTC
  15:27:33.982811 metrics.go:115: zero data points for BurstBalance
*/
