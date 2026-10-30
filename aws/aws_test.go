// Copyright 2026 Block, Inc.

package aws

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	"github.com/cashapp/blip/v2"
)

func TestConfigFactoryWrapsRegionError(t *testing.T) {
	// IMDS that fails every request, so region auto-detection fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()
	imdsOptions = imds.Options{Endpoint: server.URL}
	defer func() { imdsOptions = imds.Options{} }()

	f := &ConfigFactory{}
	_, err := f.Make(blip.AWS{Region: "auto"}, "")
	if err == nil {
		t.Fatal("got nil error, expected region auto-detection error")
	}
	if got, expect := err.Error(), "cannot auto-detect AWS region (EC2 IMDS query failed)"; got != expect {
		t.Errorf("got error %q, expected %q", got, expect)
	}
	// The IMDS HTTP response error, which has the status code
	var respErr interface{ HTTPStatusCode() int }
	if !errors.As(err, &respErr) {
		t.Fatalf("errors.As(%v, HTTPStatusCode() int) = false, expected the IMDS error", err)
	}
	if respErr.HTTPStatusCode() != http.StatusNotFound {
		t.Errorf("got IMDS status %d, expected %d", respErr.HTTPStatusCode(), http.StatusNotFound)
	}
}
