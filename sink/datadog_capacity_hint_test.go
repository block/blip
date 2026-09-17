package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"
)

func TestDatadogRawCapacityBounds(t *testing.T) {
	for _, n := range []int{0, 1, 2, 8, 9, 61, 383, 1000, 10000} {
		series := testMetricSeries(n)
		for _, target := range []int{0, 1, 511, 512, 513, 4095, 4718592, int(^uint(0) >> 1)} {
			capacity := datadogRawCapacity(series, target)
			require.Equal(t, min(n*512, target), capacity, "ordinary series retain the existing hint")
			if n == 0 {
				continue
			}
			series[n-1].Tags = []string{strings.Repeat("large", 20000)}
			capacity = datadogRawCapacity(series, target)
			require.GreaterOrEqual(t, capacity, 0)
			require.LessOrEqual(t, capacity, target)
			series[n-1] = testMetricSeries(1)[0]
		}
	}
}

func TestDatadogRawCapacityIsolatedOutlier(t *testing.T) {
	for _, position := range []int{0, 499, 999} {
		series := testMetricSeries(1000)
		series[position].Tags = []string{strings.Repeat("x", 65536)}
		require.Equal(t, 512000, datadogRawCapacity(series, datadogTargetDecompressedPayloadSize))
	}
}

func TestDatadogRawCapacityFindsLateTags(t *testing.T) {
	series := testMetricSeries(1000)
	for i := range series {
		series[i].Tags = make([]string, 64)
		series[i].Tags[63] = strings.Repeat("x", 4096)
	}
	require.Equal(t, datadogTargetDecompressedPayloadSize, datadogRawCapacity(series, datadogTargetDecompressedPayloadSize))
}

func TestDatadogCapacityHardLimitSeries(t *testing.T) {
	for _, compress := range []bool{false, true} {
		limit := datadogMaxCompressedPayloadSize
		if compress {
			limit = datadogMaxDecompressedPayloadSize
		}
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("gzip=%t/delta=%d", compress, delta), func(t *testing.T) {
				series := testMetricSeries(1)
				series[0].Tags = []string{""}
				encoded, err := json.Marshal(series[0])
				require.NoError(t, err)
				series[0].Tags[0] = strings.Repeat("x", limit+delta-len(encoded)-len(datadogPayloadPrefix)-len(datadogPayloadSuffix))
				payload, _, err := prepareDatadogPayload(context.Background(), series, 0, 10000, compress, defaultDatadogPayloadLimits())
				if delta > 0 {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, limit+delta, payload.uncompressedBytes)
				decoded, err := decodePreparedMetricPayload(payload)
				require.NoError(t, err)
				require.Equal(t, series, decoded.Series)
			})
		}
	}
}

func BenchmarkDatadogRawCapacityHint(b *testing.B) {
	for _, n := range []int{61, 383, 1000, 10000} {
		for _, tags := range []int{2, 64} {
			b.Run(fmt.Sprintf("%d/tags=%d", n, tags), func(b *testing.B) {
				series := make([]datadogV2.MetricSeries, n)
				for i := range series {
					series[i].Tags = make([]string, tags)
					for j := range series[i].Tags {
						series[i].Tags[j] = "tag:synthetic"
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					datadogRawCapacity(series, datadogTargetDecompressedPayloadSize)
				}
			})
		}
	}
}
