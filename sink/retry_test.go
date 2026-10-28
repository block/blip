// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test/mock"
)

func stack(rb *Retry) []string {
	stack := []string{}
	rb.stackMux.Lock()
	defer rb.stackMux.Unlock()
	for i := range rb.stack {
		if rb.stack[i] == nil {
			stack = append(stack, "")
		} else {
			stack = append(stack, rb.stack[i].metrics.Level)
		}
	}
	return stack
}

func TestRetry(t *testing.T) {
	sendCalled := make(chan string)
	sendReturn := make(chan error)
	mockSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			t.Logf("got %s", m.Level)
			sendCalled <- m.Level
			t.Logf("sent %s", m.Level)
			err := <-sendReturn
			t.Logf("Send error: %v", err)
			return err
		},
	}

	rb := NewRetry(RetryArgs{
		MonitorId:  "m1",
		Sink:       mockSink,
		BufferSize: 3,
	})

	// Don't need full metrics (or any metrics, really); just need different
	// metric structs to queue up to simulate slow or failing Send. Will use
	// Level to identify each struct, which is what mockSink.Send returns on
	// channel sendCalled.
	metrics1 := &blip.Metrics{Level: "1"}
	metrics2 := &blip.Metrics{Level: "2"}
	metrics3 := &blip.Metrics{Level: "3"}
	metrics4 := &blip.Metrics{Level: "4"}

	// Send first metrics in goroutine because we make it block on Send by
	// not priming sendReturn with a return value. We know Send is blocked
	// when mockSink.Send sends on channel sendCalled.
	go rb.Send(context.Background(), metrics1)
	gotMetrics := <-sendCalled
	if gotMetrics != "1" {
		t.Errorf("Send sending metrics %s, expected 1", gotMetrics)
	}

	// Now that mockSink.Sink is blocked in Send, send 3 more metrics
	rb.Send(context.Background(), metrics2)
	rb.Send(context.Background(), metrics3)
	rb.Send(context.Background(), metrics4)

	// Stack size is only 3, but we sent 4 metrics, so the stack show look like:
	//   metrics4
	//   metrics3
	//   metrics2
	// because it's a FIFO, so metrics1 got pushed off.

	got := stack(rb)
	expect := []string{"2", "3", "4"}
	assert.Equal(t, expect, got)

	// Now the fun part: mockSink.Send is blocked trying to send metrics1,
	// but as the test above ^ shows, those metrics were pushed off the stack.
	// If we make Send error by sending an error on channel sendReturn,
	// it will retry the whole stack, which means it should try sending metrics4
	// because stack if FIFO: prioritizing newer metrics over older metrics.
	sendReturn <- fmt.Errorf("Send error")
	gotMetrics = <-sendCalled
	if gotMetrics != "4" {
		t.Errorf("Send sending metrics %s, expected 4", gotMetrics)
	}

	// Let Send succeed and we should get metrics3 and metrics2, and no more

	sendReturn <- nil         // Send return for metrics4
	gotMetrics = <-sendCalled // next: metrics3
	if gotMetrics != "3" {
		t.Errorf("Send sending metrics %s, expected 3", gotMetrics)
	}

	sendReturn <- nil         // Send return for metrics3
	gotMetrics = <-sendCalled // next: metrics2
	if gotMetrics != "2" {
		t.Errorf("Send sending metrics %s, expected 2", gotMetrics)
	}

	sendReturn <- nil // Send return for metrics2

	// Stack should be empty, so these should block
	select {
	case sendReturn <- fmt.Errorf("last test"):
		t.Error("sendReturn channel not blocked")
	default:
	}
	select {
	case <-sendCalled:
		t.Error("sendCalled channel not blocked")
	default:
	}

	// Give Send a moment to pop last metrics and return
	time.Sleep(100 * time.Millisecond)

	got = stack(rb)
	expect = []string{"", "", ""}
	assert.Equal(t, expect, got)
}

func TestRetry_SendTimeoutBoundsInFlightSend(t *testing.T) {
	blockingSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	rb := NewRetry(RetryArgs{
		MonitorId:   "m1",
		Sink:        blockingSink,
		BufferSize:  3,
		SendTimeout: 200 * time.Millisecond,
	})

	done := make(chan struct{})
	go func() {
		rb.Send(context.Background(), &blip.Metrics{Level: "1"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Retry.Send stalled: SendTimeout did not bound the in-flight send")
	}
}

func TestRetry_SendTimeoutBoundsRetryWait(t *testing.T) {
	// Real sink always fails, so Send waits send-retry-wait before retrying
	failingSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			return fmt.Errorf("send failed")
		},
	}
	rb := NewRetry(RetryArgs{
		MonitorId:     "m1",
		Sink:          failingSink,
		BufferSize:    3,
		SendTimeout:   200 * time.Millisecond,
		SendRetryWait: time.Minute,
	})

	done := make(chan struct{})
	go func() {
		rb.Send(context.Background(), &blip.Metrics{Level: "1"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Retry.Send stalled: SendTimeout did not bound the retry wait")
	}
	assert.Equal(t, []string{"1", "", ""}, stack(rb))
}

func TestRetryArgsValidate(t *testing.T) {
	sink := mock.Sink{}
	valid := []RetryArgs{
		{}, // defaults
		{BufferSize: MAX_RETRY_BUFFER_SIZE, SendTimeout: MAX_RETRY_SEND_TIMEOUT, SendRetryWait: time.Hour},
	}
	for _, args := range valid {
		assert.NoError(t, args.Validate(), "%+v", args)
	}

	invalid := []RetryArgs{
		{BufferSize: MAX_RETRY_BUFFER_SIZE + 1},
		{SendTimeout: -time.Second},
		{SendTimeout: MAX_RETRY_SEND_TIMEOUT + time.Second},
		{SendRetryWait: -time.Second},
	}
	for _, args := range invalid {
		assert.Error(t, args.Validate(), "%+v", args)

		// NewRetry doesn't validate, so existing callers keep working
		args.MonitorId = "m1"
		args.Sink = sink
		assert.NotPanics(t, func() { NewRetry(args) }, "%+v", args)
	}
}

func TestRetryPopMiddle(t *testing.T) {
	// Test that popping values from the middle of the stack works. This happens
	// when, in this test for example, while sending metrics1, two more metrics
	// queue up, which pushes metrics1 from top down into stack. Once metrics1
	// sends, pop() has to pop it from the "middle" of the stack.
	sendCalled := make(chan string)
	sendReturn := make(chan error)
	mockSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			t.Logf("got %s", m.Level)
			sendCalled <- m.Level
			t.Logf("sent %s", m.Level)
			err := <-sendReturn
			t.Logf("Send error: %v", err)
			return err
		},
	}

	// Like TestRetry, just need fake metrics using Level for ID
	rb := NewRetry(RetryArgs{
		MonitorId:  "m1",
		Sink:       mockSink,
		BufferSize: 4,
	})

	metrics1 := &blip.Metrics{Level: "1"}
	metrics2 := &blip.Metrics{Level: "2"}
	metrics3 := &blip.Metrics{Level: "3"}

	// Send 3 metrics into stack of size 4
	go rb.Send(context.Background(), metrics1)
	gotMetrics := <-sendCalled
	if gotMetrics != "1" {
		t.Errorf("Send sending metrics %s, expected 1", gotMetrics)
	}
	rb.Send(context.Background(), metrics2)
	rb.Send(context.Background(), metrics3)

	// Stack looks like:
	//   <empty>
	//   metrics3
	//   metrics2
	//   metrics1
	got := stack(rb)
	expect := []string{"1", "2", "3", ""}
	assert.Equal(t, expect, got)

	// mockSink.Send is still trying to send metrics1. Unblock that by sending
	// nil error, which then makes Send re-process stack from top down, which
	// means it'll send metrics3 (the latest metrics).
	sendReturn <- nil
	gotMetrics = <-sendCalled
	if gotMetrics != "3" {
		t.Errorf("Send sending metrics %s, expected 3", gotMetrics)
	}

	// Stack looks like:
	//   <empty>
	//   <empty>
	//   metrics3
	//   metrics2
	// Although metrics1 was on top when Send tried to send it, it was on
	// bottom when Send returned, so it should be popped off from the "middle"
	// leaving the other 2 metrics in the stack.
	got = stack(rb)
	expect = []string{"2", "3", "", ""}
	assert.Equal(t, expect, got)
}

func TestRetry_NoDeltaSink(t *testing.T) {
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
		NewRetry(RetryArgs{
			MonitorId:  "m1",
			Sink:       deltaSink,
			BufferSize: 4,
		})
	}()
}

func TestRetryCountsDroppedMetrics(t *testing.T) {
	const monitorId = "retry-dropped"
	defer status.RemoveMonitor(monitorId)

	// Real sink always fails, so every Send leaves its metrics in the buffer
	mockSink := mock.Sink{
		SendFunc: func(ctx context.Context, m *blip.Metrics) error {
			return fmt.Errorf("send failed")
		},
	}
	rb := NewRetry(RetryArgs{
		MonitorId:   monitorId,
		Sink:        mockSink,
		BufferSize:  2,
		SendTimeout: 10 * time.Millisecond,
	})
	counter := status.SINK_RETRY_DROPPED + ":" + mockSink.Name()

	// Not registered yet: the 3rd Send pushes off metrics but isn't counted
	for i := 1; i <= 3; i++ {
		rb.Send(context.Background(), &blip.Metrics{Level: fmt.Sprintf("%d", i)})
	}
	if got := status.ReportCounters(monitorId); len(got) != 0 {
		t.Fatalf("counters reported before RegisterCounters: %v", got)
	}

	// Delta forwards RegisterCounters to the Retry sink it wraps
	NewDelta(rb).RegisterCounters()
	if v, ok := status.ReportCounters(monitorId)[monitorId][counter]; !ok || v != 0 {
		t.Fatalf("%s = %d (reported %t) after RegisterCounters, expected 0 and reported", counter, v, ok)
	}

	// Buffer holds 2, so the 4th and 5th Send push off the oldest metrics
	for i := 4; i <= 5; i++ {
		rb.Send(context.Background(), &blip.Metrics{Level: fmt.Sprintf("%d", i)})
	}
	assert.Equal(t, []string{"4", "5"}, stack(rb))
	if got := status.ReportCounters(monitorId)[monitorId][counter]; got != 2 {
		t.Errorf("%s = %d, expected 2", counter, got)
	}
}
