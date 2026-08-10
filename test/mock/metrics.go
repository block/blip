// Copyright 2024 Block, Inc.

package mock

import (
	"context"

	"github.com/cashapp/blip/v2"
)

var _ blip.CollectorFactory = MetricFactory{}

type MetricFactory struct {
	MakeFunc func(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error)
}

func (f MetricFactory) Make(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
	if f.MakeFunc != nil {
		return f.MakeFunc(domain, args)
	}
	return MetricsCollector{}, nil
}

// --------------------------------------------------------------------------

var _ blip.Collector = MetricsCollector{}

type MetricsCollector struct {
	PrepareFunc func(ctx context.Context, plan blip.Plan) (func(), error)
	CollectFunc func(ctx context.Context, levelName string) ([]blip.MetricValue, error)
	DomainFunc  func() string
}

func (c MetricsCollector) Domain() string {
	if c.DomainFunc != nil {
		return c.DomainFunc()
	}
	return "test"
}

func (c MetricsCollector) Help() blip.CollectorHelp {
	return blip.CollectorHelp{}
}

func (c MetricsCollector) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	if c.PrepareFunc != nil {
		return c.PrepareFunc(ctx, plan)
	}
	return nil, nil
}

func (c MetricsCollector) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	if c.CollectFunc != nil {
		return c.CollectFunc(ctx, levelName)
	}
	return nil, nil
}
