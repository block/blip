// Copyright 2024 Block, Inc.

package percona_test

import (
	"math"
	"testing"

	"github.com/cashapp/blip/v2/metrics/percona"
)

func TestPercentile(t *testing.T) {
	// MySQL uses microsecond as max query time resolution, so as long as the calculated value
	// is very close to expected value upto to 6 decimal places, it's correct
	// So we calculate the percentile value and as long as the difference between it and the expected value
	// is <= tolerance, it's correct.
	tolerance := 0.000001

	buckets := []percona.QRTBucket{
		{Time: 0.000001, Count: 0, Total: 0},
		{Time: 0.000003, Count: 0, Total: 0},
		{Time: 0.000007, Count: 21, Total: 0.000005},
		{Time: 0.000015, Count: 12694, Total: 0.153511},
		{Time: 0.00003, Count: 13669, Total: 0.257439},
		{Time: 0.000061, Count: 20159, Total: 0.927535},
		{Time: 0.000122, Count: 20541, Total: 1.660305},
		{Time: 0.000244, Count: 9700, Total: 1.671614},
		{Time: 0.000488, Count: 3938, Total: 1.245692},
		{Time: 0.000976, Count: 769, Total: 0.50613},
		{Time: 0.001953, Count: 245, Total: 0.324439},
		{Time: 0.003906, Count: 126, Total: 0.335418},
		{Time: 0.007812, Count: 52, Total: 0.286619},
		{Time: 0.015625, Count: 38, Total: 0.442354},
		{Time: 0.03125, Count: 20, Total: 0.406278},
		{Time: 0.0625, Count: 7, Total: 0.287812},
		{Time: 0.125, Count: 3, Total: 0.259817},
		{Time: 0.25, Count: 2, Total: 0.287062},
		{Time: 0.5, Count: 0, Total: 0},
		{Time: 1, Count: 0, Total: 0},
		{Time: 2, Count: 0, Total: 0},
	}

	h := percona.NewQRTHistogram(buckets)

	// List of input-output tests: firs value is percentile, second value is the
	// expected output value for that percentile (from buckets).
	tests := [][]float64{
		{0.1, 0.000012},   // P10
		{0.2, 0.000019},   // P20
		{0.3, 0.000019},   // P30
		{0.4, 0.000046},   // P40
		{0.5, 0.000046},   // P50
		{0.6, 0.000081},   // P60
		{0.7, 0.000081},   // P70
		{0.8, 0.000081},   // P80
		{0.9, 0.000172},   // P90
		{0.95, 0.000316},  // P95
		{0.99, 0.000658},  // P99
		{0.999, 0.005511}, // P999
		{1, 0.143531},     // P100
	}
	var percentile, expected float64

	for _, test := range tests {
		percentile, expected = test[0], test[1]
		result, _ := h.Percentile(percentile)
		if math.Abs(result-expected) > tolerance {
			t.Errorf("For Percentile: %v\tExpected: %v\tGot: %v\n", percentile, expected, result)
		}
	}
}
