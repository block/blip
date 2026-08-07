// Copyright 2024 Block, Inc.

// Package test provides helper functions for tests.
package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/cashapp/blip/v2"
)

// blip.Confg* bool are *bool, so test.True is a convenience var
var t = true
var True = &t

var Headers = map[string]string{}

// MakeHTTPRequest is a helper function for making an http request. The
// response body of the http request is unmarshalled into the struct pointed to
// by the respStruct argument (if it's not nil). The status code of the
// response is returned.
func MakeHTTPRequest(httpVerb, url string, payload []byte, respStruct interface{}) (int, error) {
	var statusCode int

	// Make the http request.
	req, err := http.NewRequest(httpVerb, url, bytes.NewReader(payload))
	if err != nil {
		return statusCode, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range Headers {
		req.Header.Set(k, v)
	}
	res, err := (http.DefaultClient).Do(req)
	if err != nil {
		return statusCode, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)

	// Decode response into respSruct
	if respStruct != nil && len(body) > 0 {
		if err := json.Unmarshal(body, &respStruct); err != nil {
			return statusCode, fmt.Errorf("Can't decode response body: %s: %s", err, string(body))
		}
	}

	statusCode = res.StatusCode

	return statusCode, nil
}

type planFile map[string]*blip.Level

// ReadPlan is nearly the same as plan.ReadFile but recreated in pkg test
// because it creates an import cycle like: metrics/foo imports test,
// which imports plan, which imports metrics/, which importas all the metrics
// include metrics/foo. It's more important for plan to import metrics,
// so it can do plan validation, so we work around the issue here instead.
func ReadPlan(t *testing.T, file string) blip.Plan {
	if file == "" {
		file = "../../test/plans/default.yaml"
	}

	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}

	var pf planFile
	if err := yaml.Unmarshal(bytes, &pf); err != nil {
		t.Fatal(err)
	}

	levels := make(map[string]blip.Level, len(pf))
	for k := range pf {
		levels[k] = blip.Level{
			Name:    k, // must have, levels are collected by name
			Freq:    pf[k].Freq,
			Collect: pf[k].Collect,
		}
	}

	return blip.Plan{
		Name:   file,
		Levels: levels,
	}
}
