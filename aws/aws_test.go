// Copyright 2026 Block, Inc.

package aws

import (
	"context"
	"errors"
	"testing"
)

func TestRegionErrorWrapsIMDSError(t *testing.T) {
	var err error = regionError{err: context.DeadlineExceeded}
	if got, expect := err.Error(), "cannot auto-detect AWS region (EC2 IMDS query failed)"; got != expect {
		t.Errorf("got error %q, expected %q", got, expect)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(%v, context.DeadlineExceeded) = false, expected true", err)
	}
}
