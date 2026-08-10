// Copyright 2024 Block, Inc.

package sqlutil_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2/sqlutil"
)

func TestParsePercentileStr(t *testing.T) {
	tolerance := 0.000001
	testCases := map[string]float64{
		"0.999": 0.999,
		"99.9":  0.999,
		"999":   0.999,
	}

	for percentileStr, expectedResult := range testCases {
		actualResult, err := sqlutil.ParsePercentileStr(percentileStr)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(actualResult-expectedResult) > tolerance {
			t.Errorf("Expected result %f for ParsePercentileStr(%s) but got %f", expectedResult, percentileStr, actualResult-expectedResult)
		}
	}
}

func TestFormatPercentile(t *testing.T) {
	testCases := map[float64]string{
		0.99:  "p99",
		0.999: "p999",
		0.90:  "p90",
	}

	for percentile, expectedFormat := range testCases {
		actualFormat := sqlutil.FormatPercentile(percentile)
		if actualFormat != expectedFormat {
			t.Errorf("Expected format %s for FormatPercentile(%f) but got %s", expectedFormat, percentile, actualFormat)
		}
	}
}

func TestPercentileMetrics(t *testing.T) {
	got, err := sqlutil.PercentileMetrics([]string{
		"p1",
		"p10",
		"p50",
		"p95",
		"p99",
		"P99",
		"p999",
		"p100",
	})
	assert.Nil(t, err)

	expect := []sqlutil.P{
		{Name: "p1", Value: 0.01},
		{Name: "p10", Value: 0.10},
		{Name: "p50", Value: 0.5},
		{Name: "p95", Value: 0.95},
		{Name: "p99", Value: 0.99},
		{Name: "p99", Value: 0.99},
		{Name: "p999", Value: 0.999},
		{Name: "p100", Value: 1.0},
	}
	assert.Equal(t, expect, got)
}
