// Copyright 2024 Block, Inc.

package errors

import (
	"github.com/cashapp/blip/v2"
)

type TruncateErrorPolicy struct {
	Policy           Policy
	hadTruncateError bool
}

func NewTruncateErrorPolicy(s string) *TruncateErrorPolicy {
	p := NewPolicy(s)
	return &TruncateErrorPolicy{
		Policy: *p,
	}
}

// Handles errors related to truncating tables, specifically those in `performance_schema` where `TRUNCATE` is used to
// reset the collected performance metrics. This should be called to proces the result of a `TRUNCATE` query even if
// no error occurrred. It will handle adjusting the collected metric values as necessary based on the success or failure
// of `TRUNCATE` so that only metrics that were collected from a sample that had a valid collection interval are returned.
func (p *TruncateErrorPolicy) TruncateError(err error, stop *bool, collectedMetrics []blip.MetricValue) ([]blip.MetricValue, error) {
	// Track the state of errors from attempting to truncate. We need to determine if
	// we have recovered from a prior error or not.
	if err == nil && !p.hadTruncateError {
		// We are not recovering from an error and truncation was fine, so we should return the collected metrics as-is
		return collectedMetrics, nil
	}

	// Stop trying to collect if error policy retry="stop". This affects
	// future calls to Collect; don't return yet because we need to check
	// the metric policy: drop or zero. If zero, we must report one zero val.
	if p.Policy.Retry == POLICY_RETRY_NO {
		*stop = true
	}

	// Report
	var reportedErr error
	if p.Policy.ReportError() {
		reportedErr = err
	} else {
		blip.Debug("error policy=ignore: %v", err)
	}

	// We may need to change the resulting metric values depending on if we had an error with truncation or not.
	// If this is the first instance of a truncation error then the metrics are fine as-is. Once truncation fails
	// subsequent metrics will have accumulated in the `performance_schema` table over a different interval than
	// prior samples. In those cases we want to apply our error policy for metrics to the collected values.
	// We will continue to apply the error processing to metrics until truncation succeeds and the interval of the
	// collected values returns to the expected value.
	processMetrics := p.hadTruncateError
	p.hadTruncateError = (err != nil)

	var metrics []blip.MetricValue
	if processMetrics {
		if p.Policy.Metric == POLICY_METRIC_ZERO {
			metrics = make([]blip.MetricValue, 0, len(collectedMetrics))
			for _, existingMetric := range collectedMetrics {
				metrics = append(metrics, blip.MetricValue{
					Type:  existingMetric.Type,
					Name:  existingMetric.Name,
					Value: 0,
				})
			}
		}
	} else {
		metrics = collectedMetrics
	}

	return metrics, reportedErr
}
