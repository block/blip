// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
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
