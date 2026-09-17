// Copyright 2026 Block, Inc.

package aws

import "testing"

func TestNewRDSCAPool(t *testing.T) {
	first, err := NewRDSCAPool()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRDSCAPool()
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Fatal("NewRDSCAPool returned a shared certificate pool")
	}
	if got := len(first.Subjects()); got != 108 {
		t.Fatalf("embedded AWS RDS CA bundle contains %d certificates, expected 108", got)
	}
}
