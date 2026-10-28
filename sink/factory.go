// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/cashapp/blip/v2"
)

// Default is the default sink if config.sinks is not specified.
var Default = "log"

func Register(name string, f blip.SinkFactory) error {
	r.Lock()
	defer r.Unlock()
	_, ok := r.factory[name]
	if ok {
		return fmt.Errorf("sink %s already registered", name)
	}
	r.factory[name] = f
	blip.Debug("register sink %s", name)
	return nil
}

func List() []string {
	r.Lock()
	defer r.Unlock()
	names := []string{}
	for k := range r.factory {
		names = append(names, k)
	}
	return names
}

func Make(args blip.SinkFactoryArgs) (blip.Sink, error) {
	r.Lock()
	defer r.Unlock()
	f, ok := r.factory[args.SinkName]
	if !ok {
		return nil, fmt.Errorf("sink %s not registered", args.SinkName)
	}
	return f.Make(args)
}

// --------------------------------------------------------------------------

type noopSink struct{}

func (s noopSink) Send(ctx context.Context, m *blip.Metrics) error {
	return nil
}

func (s noopSink) Status() string {
	return ""
}

func (s noopSink) Name() string {
	return "noop"
}

var noop = noopSink{}

// --------------------------------------------------------------------------

func init() {
	Register("datadog", f)
	Register("chronosphere", f)
	Register("signalfx", f)
	Register("log", f)
	Register("noop", f)
	Register("prom-pushgateway", f)
}

type repo struct {
	*sync.Mutex
	factory map[string]blip.SinkFactory
}

var r = &repo{
	Mutex:   &sync.Mutex{},
	factory: map[string]blip.SinkFactory{},
}

type factory struct {
	HTTPClient blip.HTTPClientFactory
}

var f = &factory{}

func InitFactory(factories blip.Factories) {
	f.HTTPClient = factories.HTTPClient
}

func (f *factory) Make(args blip.SinkFactoryArgs) (blip.Sink, error) {
	// Return early for sinks with no options
	if args.SinkName == "log" {
		return NewLogSink(args.MonitorId)
	}
	if args.SinkName == "noop" {
		return noop, nil
	}

	// ----------------------------------------------------------------------
	// Built-in sinks use Retry to serialize access and retry on Send error.
	// First build the specific sink, then return it wrapped in Retry.

	// Parse retry options. Retry options are consumed here, so delete them
	// from the options passed to the specific sink, which rejects unknown options.
	retryArgs := RetryArgs{
		MonitorId: args.MonitorId,
	}
	// Parse in a fixed order, not map order, so the same invalid options always
	// return the same error.
	sinkOpts := maps.Clone(args.Options)
	for _, k := range []string{"buffer-size", "send-timeout", "send-retry-wait"} {
		v, ok := sinkOpts[k]
		if !ok {
			continue
		}
		var err error
		switch k {
		case "buffer-size":
			retryArgs.BufferSize, err = parseRetryBufferSize(v)
		case "send-timeout":
			retryArgs.SendTimeout, err = parseRetryDuration(v)
		case "send-retry-wait":
			retryArgs.SendRetryWait, err = parseRetryDuration(v)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid %s sink option %s: %w", args.SinkName, k, err)
		}
		delete(sinkOpts, k)
	}
	if err := retryArgs.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s sink option %w", args.SinkName, err)
	}

	// Make specific built-in sink
	var err error
	switch args.SinkName {
	case "chronosphere":
		retryArgs.Sink, err = NewChronosphere(args.MonitorId, sinkOpts, args.Tags)
	case "signalfx":
		httpClient, err := f.HTTPClient.MakeForSink("signalfx", args.MonitorId, sinkOpts, args.Tags)
		if err != nil {
			return nil, err
		}
		retryArgs.Sink, err = NewSignalFx(args.MonitorId, sinkOpts, args.Tags, httpClient)
		if err != nil {
			return nil, err
		}
	case "datadog":
		httpClient, err := f.HTTPClient.MakeForSink("datadog", args.MonitorId, sinkOpts, args.Tags)
		if err != nil {
			return nil, err
		}
		retryArgs.Sink, err = NewDatadog(args.MonitorId, sinkOpts, args.Tags, httpClient)
		if err != nil {
			return nil, err
		}
	case "prom-pushgateway":
		retryArgs.Sink, err = NewPromPushgateway(args.MonitorId, sinkOpts, args.Tags)
	default:
		return nil, fmt.Errorf("sink %s not registered", args.SinkName)
	}
	if err != nil {
		return nil, err
	}

	// Wrap the sink as needed. All sinks should be wrapped with the
	// built-in Retry sink, but some need to calculate delta
	// versions for counters, which should wrap the Retry sink
	switch args.SinkName {
	case "datadog":
		return NewDelta(NewRetry(retryArgs)), nil
	default:
		return NewRetry(retryArgs), nil
	}
}

// parseRetryBufferSize parses a buffer-size option, which must be greater than
// zero. RetryArgs.Validate checks the maximum.
func parseRetryBufferSize(v string) (uint, error) {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("%d: must be greater than zero", n)
	}
	return uint(n), nil
}

// parseRetryDuration parses a send-timeout or send-retry-wait option, which must
// be greater than zero. RetryArgs.Validate checks the maximum.
func parseRetryDuration(v string) (time.Duration, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s: must be greater than zero", v)
	}
	return d, nil
}
