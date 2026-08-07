package sink

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test/mock"
	"github.com/go-test/deep"
)

func getNoChangesValues() []blip.MetricValue {
	return []blip.MetricValue{
		{
			Name:  "gauge",
			Value: 1.0,
			Type:  blip.GAUGE,
		},
		{
			Name:  "delta",
			Value: 1.0,
			Type:  blip.DELTA_COUNTER,
		},
	}
}

func getChangesValues() []blip.MetricValue {
	return []blip.MetricValue{
		{
			Name:  "counter",
			Value: 1.0,
			Type:  blip.CUMULATIVE_COUNTER,
			Group: map[string]string{
				"a": "1",
			},
		},
		{
			Name:  "counter",
			Value: 1.0,
			Type:  blip.CUMULATIVE_COUNTER,
			Group: map[string]string{
				"a": "2",
			},
		},
		{
			Name:  "delta",
			Value: 1.0,
			Type:  blip.DELTA_COUNTER,
		},
	}
}

func TestDeltaSink(t *testing.T) {
	var returnedResults *blip.Metrics
	mockSink := &mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			returnedResults = m
			return nil
		},
	}

	noChangesValues := getNoChangesValues()
	metrics := &blip.Metrics{
		Begin:     time.Now().Add(-1 * time.Hour),
		End:       time.Now(),
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		Interval:  42,
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"nochanges": noChangesValues,
			"changes":   getChangesValues(),
		},
	}

	deltaSink := NewDelta(mockSink)
	err := deltaSink.Send(context.Background(), metrics)
	if err != nil {
		t.Error(err)
	}

	// If we had to perform delta calculations then we will get a pointer to a new
	// set of metrics.
	if metrics == returnedResults {
		t.Error("Expected returnedResults and metrics to be different pointers but they matched")
	}

	// The first time we submit cumulative metrics we should not expect to see them
	// in the output as we didn't have enough data points to calculate deltas. The
	// only deltas should be those submitted as delta values.
	expectedMetrics := &blip.Metrics{
		Begin:     metrics.Begin,
		End:       metrics.End,
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		Interval:  metrics.Interval,
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"nochanges": noChangesValues,
			"changes": {
				{
					Name:  "delta",
					Value: 1.0,
					Type:  blip.DELTA_COUNTER,
				},
			},
		},
	}

	if diff := deep.Equal(expectedMetrics, returnedResults); diff != nil {
		t.Error(diff)
	}

	// Update and send new metrics
	// The cumulative counters should have their values increased
	// so we get valid deltas.
	changesValues := getChangesValues()
	changesValues[0].Value = 2.0
	changesValues[1].Value = 3.0

	metrics = &blip.Metrics{
		Begin:     time.Now().Add(-1 * time.Hour),
		End:       time.Now(),
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		Interval:  43,
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"nochanges": noChangesValues,
			"changes":   changesValues,
		},
	}

	err = deltaSink.Send(context.Background(), metrics)
	if err != nil {
		t.Error(err)
	}

	if metrics == returnedResults {
		t.Error("Expected returnedResults and metrics to be different pointers but they matched")
	}

	// We should see the newly calculated delta values now that we had a prior run
	expectedMetrics = &blip.Metrics{
		Begin:     metrics.Begin,
		End:       metrics.End,
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		Interval:  metrics.Interval,
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"nochanges": noChangesValues,
			"changes": {
				{
					Name:  "counter",
					Value: 1.0,
					Type:  blip.DELTA_COUNTER,
					Group: map[string]string{
						"a": "1",
					},
				},
				{
					Name:  "counter",
					Value: 2.0,
					Type:  blip.DELTA_COUNTER,
					Group: map[string]string{
						"a": "2",
					},
				},
				{
					Name:  "delta",
					Value: 1.0,
					Type:  blip.DELTA_COUNTER,
				},
			},
		},
	}

	if diff := deep.Equal(expectedMetrics, returnedResults); diff != nil {
		t.Error(diff)
	}
}

func TestDeltaSinkSeriesIdentity(t *testing.T) {
	tests := []struct {
		name   string
		values map[string][]blip.MetricValue
	}{
		{
			name: "domain",
			values: map[string][]blip.MetricValue{
				"domain-1": {{Name: "counter", Value: 10, Type: blip.CUMULATIVE_COUNTER}},
				"domain-2": {{Name: "counter", Value: 20, Type: blip.CUMULATIVE_COUNTER}},
			},
		},
		{
			name: "group keys",
			values: map[string][]blip.MetricValue{
				"domain": {
					{Name: "counter", Value: 10, Type: blip.CUMULATIVE_COUNTER, Group: map[string]string{"a": "1"}},
					{Name: "counter", Value: 20, Type: blip.CUMULATIVE_COUNTER, Group: map[string]string{"b": "1"}},
				},
			},
		},
		{
			name: "group value boundaries",
			values: map[string][]blip.MetricValue{
				"domain": {
					{Name: "counter", Value: 10, Type: blip.CUMULATIVE_COUNTER, Group: map[string]string{"a": "12", "b": "3"}},
					{Name: "counter", Value: 20, Type: blip.CUMULATIVE_COUNTER, Group: map[string]string{"a": "1", "b": "23"}},
				},
			},
		},
		{
			name: "metric name boundary",
			values: map[string][]blip.MetricValue{
				"domain": {
					{Name: "counter-1", Value: 10, Type: blip.CUMULATIVE_COUNTER, Group: map[string]string{"a": "23"}},
					{Name: "counter-12", Value: 20, Type: blip.CUMULATIVE_COUNTER, Group: map[string]string{"a": "3"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *blip.Metrics
			deltaSink := NewDelta(&mock.Sink{SendFunc: func(_ context.Context, m *blip.Metrics) error {
				got = m
				return nil
			}})

			if err := deltaSink.Send(context.Background(), &blip.Metrics{Values: tt.values}); err != nil {
				t.Fatal(err)
			}

			for domain, values := range got.Values {
				if len(values) != 0 {
					t.Errorf("first sample for %s emitted %d values; want none: %+v", domain, len(values), values)
				}
			}
		})
	}
}

func TestDeltaSinkConcurrentSend(t *testing.T) {
	const sends = 100

	var (
		mu       sync.Mutex
		received = make(map[string]float64)
	)
	deltaSink := NewDelta(&mock.Sink{SendFunc: func(_ context.Context, m *blip.Metrics) error {
		mu.Lock()
		defer mu.Unlock()
		for _, values := range m.Values {
			for _, value := range values {
				received[value.Group["id"]] = value.Value
			}
		}
		return nil
	}})

	sendAll := func(value float64) {
		t.Helper()
		var wg sync.WaitGroup
		for i := 0; i < sends; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("counter-%d", i)
				err := deltaSink.Send(context.Background(), &blip.Metrics{Values: map[string][]blip.MetricValue{
					"domain": {{
						Name:  "counter",
						Value: value,
						Type:  blip.CUMULATIVE_COUNTER,
						Group: map[string]string{"id": id},
					}},
				}})
				if err != nil {
					t.Errorf("send %s: %v", id, err)
				}
			}(i)
		}
		wg.Wait()
	}

	sendAll(10)
	sendAll(11)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != sends {
		t.Fatalf("received %d deltas; want %d", len(received), sends)
	}
	for id, value := range received {
		if value != 1 {
			t.Errorf("%s delta = %v; want 1", id, value)
		}
	}
}

func TestDeltaSinkCounterReset(t *testing.T) {
	var got *blip.Metrics
	deltaSink := NewDelta(&mock.Sink{SendFunc: func(_ context.Context, m *blip.Metrics) error {
		got = m
		return nil
	}})

	send := func(value float64) {
		t.Helper()
		if err := deltaSink.Send(context.Background(), &blip.Metrics{Values: map[string][]blip.MetricValue{
			"domain": {{Name: "counter", Value: value, Type: blip.CUMULATIVE_COUNTER}},
		}}); err != nil {
			t.Fatal(err)
		}
	}

	send(100)
	send(5)
	if value := got.Values["domain"][0].Value; value != 5 {
		t.Fatalf("reset delta = %v; want current partial value 5", value)
	}
	send(8)
	if value := got.Values["domain"][0].Value; value != 3 {
		t.Fatalf("post-reset delta = %v; want 3", value)
	}
}

func TestDeltaSink_Passthrough(t *testing.T) {
	var returnedResults *blip.Metrics
	mockSink := &mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			returnedResults = m
			return nil
		},
	}

	metrics := &blip.Metrics{
		Begin:     time.Now().Add(-1 * time.Hour),
		End:       time.Now(),
		MonitorId: "testmonitor",
		Plan:      "testplan",
		Level:     "testlevel",
		State:     "teststate",
		Values: map[string][]blip.MetricValue{
			"nochanges": getNoChangesValues(),
		},
	}

	deltaSink := NewDelta(mockSink)
	err := deltaSink.Send(context.Background(), metrics)
	if err != nil {
		t.Error(err)
	}

	// If the metrics don't contain any cumulative metrics then we should
	// just get the same metrics pointer returned as no transformations needed to happen.
	if metrics != returnedResults {
		t.Error("Expected returnedResults and metrics to be the same pointers but they are different")
	}
}

func TestDelta_NoDeltaSink(t *testing.T) {
	mockSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			return nil
		},
	}

	deltaSink := NewDelta(mockSink)

	func() {
		defer func() {
			if err := recover(); err == nil {
				t.Error("Expected an error but didn't get one")
			}
		}()

		// Create the Retry sink with a Delta sink, which isn't allowed.
		NewDelta(deltaSink)
	}()
}

func TestDelta_NoNilSink(t *testing.T) {
	func() {
		defer func() {
			if err := recover(); err == nil {
				t.Error("Expected an error but didn't get one")
			}
		}()

		// Create the Retry sink with a Delta sink, which isn't allowed.
		NewDelta(nil)
	}()
}
