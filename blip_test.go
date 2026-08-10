// Copyright 2024 Block, Inc.

package blip_test

import (
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
)

func TestTimeLimit(t *testing.T) {
	var testCases = []struct {
		p   float64
		in  time.Duration
		max time.Duration
		out time.Duration
	}{
		{
			0.20,                                  // 20% off
			time.Duration(1) * time.Second,        // 1s
			time.Duration(1) * time.Second,        // up to 1s
			time.Duration(800) * time.Millisecond, // = 800ms
		},
		{
			0.20,                            // 20% off
			time.Duration(10) * time.Second, // 10s
			time.Duration(1) * time.Second,  // up to 1s
			time.Duration(9) * time.Second,  // = 9s
		},

		// Not normal usage, but used for fast testing:
		{
			0.20,                                  // 20% off
			time.Duration(100) * time.Millisecond, // 100ms
			time.Duration(20) * time.Millisecond,  // up to 20ms
			time.Duration(80) * time.Millisecond,  // = 80ms
		},
	}

	for _, tc := range testCases {
		got := blip.TimeLimit(tc.p, tc.in, tc.max)
		if got != tc.out {
			t.Errorf("TimeLimit(%v, %v, %v) = %v, expected %v", tc.p, tc.in, tc.max, got, tc.out)
		}
	}
}
