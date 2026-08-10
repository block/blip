// Copyright 2024 Block, Inc.

package errors_test

import (
	"fmt"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/errors"
	"github.com/go-test/deep"
)

const (
	TRUNCATE_ERR_STR = "Truncate Error"
)

func TestTruncateErrorZero(t *testing.T) {
	t.Parallel()
	collectedMetrics := []blip.MetricValue{
		{
			Name:  "Metric1",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 2.0,
		},
		{
			Name:  "Metric2",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 2.0,
		},
	}
	zeroMetrics := []blip.MetricValue{
		{
			Name:  "Metric1",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 0.0,
		},
		{
			Name:  "Metric2",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 0.0,
		},
	}

	testErr := fmt.Errorf(TRUNCATE_ERR_STR)
	stopValue := false
	truncatePolicy := errors.NewTruncateErrorPolicy("report,zero,retry")

	returnedMetrics, err := truncatePolicy.TruncateError(nil, &stopValue, collectedMetrics)
	if err != nil {
		t.Errorf("Expected no error but got: %v", err)
	}
	if stopValue {
		t.Errorf("Stop value should be false but it is true")
	}
	if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
		t.Error(diff)
	}

	for i := 0; i < 5; i++ {
		returnedMetrics, err = truncatePolicy.TruncateError(testErr, &stopValue, collectedMetrics)
		if stopValue {
			t.Errorf("Stop value should be false but it is true")
		}

		if testErr != nil {
			if err == nil {
				t.Errorf("Expected an error but got none")
			} else if err.Error() != TRUNCATE_ERR_STR {
				t.Errorf("Expected %s but got %v", TRUNCATE_ERR_STR, err)
			}
		} else {
			if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		}

		// The first time we get an error we should still have good metrics. Later
		// iterations should result in metrics with "0" values due to the sampling
		// interval no longer being correct as a result of truncation errors.
		if i == 0 {
			if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
				t.Errorf("Iteration %d: %v", i, diff)
			}
		} else if i < 4 {
			if diff := deep.Equal(zeroMetrics, returnedMetrics); diff != nil {
				t.Errorf("Iteration %d: %v", i, diff)
			}
		} else {
			// Once we properly recover we should start seeing metrics again
			if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
				t.Errorf("Iteration %d: %v", i, diff)
			}
		}

		// Simulate the truncation failure recovering
		if i == 2 {
			testErr = nil
		}
	}
}

func TestTruncateErrorZeroDrop(t *testing.T) {
	t.Parallel()
	collectedMetrics := []blip.MetricValue{
		{
			Name:  "Metric1",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 2.0,
		},
		{
			Name:  "Metric2",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 2.0,
		},
	}

	testErr := fmt.Errorf(TRUNCATE_ERR_STR)
	stopValue := false
	truncatePolicy := errors.NewTruncateErrorPolicy("report,drop,retry")

	returnedMetrics, err := truncatePolicy.TruncateError(nil, &stopValue, collectedMetrics)
	if err != nil {
		t.Errorf("Expected no error but got: %v", err)
	}
	if stopValue {
		t.Errorf("Stop value should be false but it is true")
	}
	if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
		t.Error(diff)
	}

	for i := 0; i < 5; i++ {
		returnedMetrics, err = truncatePolicy.TruncateError(testErr, &stopValue, collectedMetrics)
		if stopValue {
			t.Errorf("Stop value should be false but it is true")
		}

		if testErr != nil {
			if err == nil {
				t.Errorf("Expected an error but got none")
			} else if err.Error() != TRUNCATE_ERR_STR {
				t.Errorf("Expected %s but got %v", TRUNCATE_ERR_STR, err)
			}
		} else {
			if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		}

		// The first time we get an error we should still have good metrics. Later
		// iterations should result in metrics being dropped due to the sampling
		// interval no longer being correct as a result of truncation errors.
		if i == 0 {
			if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
				t.Errorf("Iteration %d: %v", i, diff)
			}
		} else if i < 4 {
			if returnedMetrics != nil {
				t.Errorf("Expected no metrics but got %+v", returnedMetrics)
			}
		} else {
			// Once we properly recover we should start seeing metrics again
			if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
				t.Errorf("Iteration %d: %v", i, diff)
			}
		}

		// Simulate the truncation failure recovering
		if i == 2 {
			testErr = nil
		}
	}
}

func TestTruncateErrorZeroStopIgnore(t *testing.T) {
	t.Parallel()
	collectedMetrics := []blip.MetricValue{
		{
			Name:  "Metric1",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 2.0,
		},
		{
			Name:  "Metric2",
			Type:  blip.CUMULATIVE_COUNTER,
			Value: 2.0,
		},
	}

	testErr := fmt.Errorf(TRUNCATE_ERR_STR)
	stopValue := false
	truncatePolicy := errors.NewTruncateErrorPolicy("ignore,drop,stop")

	returnedMetrics, err := truncatePolicy.TruncateError(nil, &stopValue, collectedMetrics)
	if err != nil {
		t.Errorf("Expected no error but got: %v", err)
	}
	if stopValue {
		t.Errorf("Stop value should be false but it is true")
	}
	if diff := deep.Equal(collectedMetrics, returnedMetrics); diff != nil {
		t.Error(diff)
	}

	returnedMetrics, err = truncatePolicy.TruncateError(testErr, &stopValue, collectedMetrics)
	if !stopValue {
		t.Errorf("Stop value should be true but it is false")
	}
	if err != nil {
		t.Errorf("Expected no error but got: %v", err)
	}
}
