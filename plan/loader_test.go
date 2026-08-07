// Copyright 2024 Block, Inc.

package plan_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"

	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/test/mock"
)

// --------------------------------------------------------------------------

func TestLoadDefault(t *testing.T) {
	cfg := blip.DefaultConfig()

	pl := plan.NewLoader(nil)
	if err := pl.LoadShared(cfg.Plans, nil); err != nil {
		t.Fatal(err)
	}

	gotPlans := pl.SharedPlans()
	expectPlans := []plan.Meta{
		{
			Name:   "default-mysql",
			Source: "blip",
			Shared: true,
		},
		{
			Name:   blip.DEFAULT_EXPORTER_PLAN,
			Source: "blip",
			Shared: true,
		},
	}
	for i := range gotPlans {
		if gotPlans[i].YAML == "" {
			t.Errorf("%s missing YAML", gotPlans[i].Name)
		}
		gotPlans[i].YAML = ""
	}
	if diff := deep.Equal(gotPlans, expectPlans); diff != nil {
		t.Error(diff)
	}
}

func TestLoadOneFile(t *testing.T) {
	file := "../test/plans/version.yaml"
	fileabs, err := filepath.Abs(file)
	if err != nil {
		t.Fatal(err)
	}

	cfg := blip.Config{
		Plans:    blip.ConfigPlans{Files: []string{file}},
		Monitors: []blip.ConfigMonitor{},
	}

	pl := plan.NewLoader(nil)
	if err := pl.LoadShared(cfg.Plans, nil); err != nil {
		t.Fatal(err)
	}

	gotPlans := pl.SharedPlans()
	expectPlans := []plan.Meta{
		{
			Name:   file,
			Source: fileabs,
		},
	}
	for i := range gotPlans {
		if gotPlans[i].YAML == "" {
			t.Errorf("%s missing YAML", gotPlans[i].Name)
		}
		gotPlans[i].YAML = ""
	}
	if diff := deep.Equal(gotPlans, expectPlans); diff != nil {
		t.Error(diff)
	}
}

// TestPlanShouldReturnDeepCopyOfPlan needs to ensure that the copy of blip.Plan returned is
// indeed a deep copy of the struct with new copies of all reference types created, such as
// slice and  map fields. This is important because the plan_loader cannot control what callers
// do with the blip.Plan as they sometimes need to modify to do effective work. A real world
// example of this would be that the level_collecter logic needs to sort the plan and aggregate
// metrics into levels across divisible freqencies. If the same plan needs to be changed again
// , say by the level_adjuster, then the returned plan, if it is not a deep copy, would contain
// metrics with aggregations that were not part of the original plan. If this modified plan were
// to be passed to the level_collector again , the resulting behavior would be considered
// undefined and in this example would introduce bugs such as duplicate metrics.
func TestPlanShouldReturnDeepCopyOfPlan(t *testing.T) {
	mc := mock.MetricsCollector{
		CollectFunc: func(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
			return nil, nil
		},
	}
	mf := mock.MetricFactory{
		MakeFunc: func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
			return mc, nil
		},
	}
	metrics.Register(mc.Domain(), mf) // MUST CALL FIRST, before the rest...

	planName := "foobar"
	expected := blip.Plan{
		Name: planName,
		Levels: map[string]blip.Level{
			"l1": {
				Name: "l1",
				Freq: "1s",
				Collect: map[string]blip.Domain{
					"test": {
						Name:    "d1",
						Metrics: []string{"m1"},
					},
				},
			},
			"l2": {
				Name: "l2",
				Freq: "5s",
				Collect: map[string]blip.Domain{
					"test": {
						Name:    "d1",
						Metrics: []string{"m2"},
					},
				},
			},
		},
	}
	pl := plan.NewLoader(
		func(blip.ConfigPlans) ([]blip.Plan, error) {
			return []blip.Plan{expected}, nil
		})
	err := pl.LoadShared(blip.ConfigPlans{}, nil)
	assert.Nil(t, err)

	got, err := pl.Plan("", planName, nil)
	assert.Nil(t, err)

	assert.Equal(t, expected, got)
	// Verify that is a is a deep copy of b by comparing address of the slices and maps in blip.Plan.
	// This solution is not ideal, but there really isn't a good way to compare reference object addresses in go.
	// The converse is to attempt to mutate the slices and maps embedded within blip.Plan, but even more verbose.
	// Verify top level map.
	assert.NotEqual(t, fmt.Sprintf("%p", expected.Levels), fmt.Sprintf("%p", got.Levels))
	for levelKey, expectedLevel := range expected.Levels {
		// Verify domain maps.
		expectedCollect := expectedLevel.Collect
		gotCollect := got.Levels[levelKey].Collect
		assert.NotEqual(t, fmt.Sprintf("%p", expectedCollect), fmt.Sprintf("%p", gotCollect))
		for domainKey, expectedDomain := range expectedCollect {
			// Verify slice of metrics to collect.
			gotDomain := gotCollect[domainKey]
			assert.NotEqual(t, fmt.Sprintf("%p", expectedDomain.Metrics), fmt.Sprintf("%p", gotDomain.Metrics))
		}
	}
}
