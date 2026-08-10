// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"sync"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
)

const (
	DEFAULT_RETRY_BUFFER_SIZE     = 60
	DEFAULT_RETRY_SEND_TIMEOUT    = "5s"
	DEFAULT_RETRY_SEND_RETRY_WAIT = "200ms"
)

// Retry is a pseudo-sink that provides buffering, serialization, and retry for
// a real sink. The built-in sinks, except "log", use Retry to handle those three
// complexities.
//
// Retry uses a LIFO queue (a stack) to prioritize sending the latest metrics.
// This means that, during a long outage of the real sink, Retry drops the oldest
// metrics and keeps the latest metrics, up to its buffer size, which is configurable.
//
// Retry sends SINK_SEND_ERROR events on Send error; the real sink should not.
type Retry struct {
	sink blip.Sink

	sendMux     *sync.Mutex
	sending     bool
	sendTimeout time.Duration
	retryWait   time.Duration

	event event.MonitorReceiver

	stackMux *sync.Mutex
	stack    []*retryItem // LIFO
	max      int
	top      int
}

// CheckpointSink can resume a partially acknowledged metrics batch. Retry
// stores the opaque checkpoint with the same queue entry as the metrics. A
// sink must advance the returned checkpoint only after remote acknowledgement.
type CheckpointSink interface {
	SendWithCheckpoint(context.Context, *blip.Metrics, any) (any, error)
}

type retryItem struct {
	metrics    *blip.Metrics
	checkpoint any
}

type RetryArgs struct {
	MonitorId     string        // required
	Sink          blip.Sink     // required
	BufferSize    uint          // optional; DEFAULT_RETRY_BUFFER_SIZE
	SendTimeout   time.Duration // optional; DEFAULT_RETRY_SEND_TIMEOUT
	SendRetryWait time.Duration // optional; DEFAULT_RETRY_SEND_RETRY_WAIT
}

func NewRetry(args RetryArgs) *Retry {
	// Panic if caller doesn't provide required args
	if args.MonitorId == "" {
		panic("RetryArgs.MonitorId is empty string; value required")
	}
	if args.Sink == nil {
		panic("RetryArgs.Sink is nil; value required")
	}

	if _, ok := args.Sink.(*Delta); ok {
		panic("RetryArgs.Sink cannot be a Delta sink.")
	}

	// Set defaults
	if args.BufferSize == 0 {
		args.BufferSize = DEFAULT_RETRY_BUFFER_SIZE
	}
	if args.SendTimeout == 0 {
		args.SendTimeout, _ = time.ParseDuration(DEFAULT_RETRY_SEND_TIMEOUT)
	}
	if args.SendRetryWait == 0 {
		args.SendRetryWait, _ = time.ParseDuration(DEFAULT_RETRY_SEND_RETRY_WAIT)
	}

	rb := &Retry{
		sink:  args.Sink,
		event: event.MonitorReceiver{MonitorId: args.MonitorId},

		sendMux:     &sync.Mutex{},
		sending:     false,
		sendTimeout: args.SendTimeout,
		retryWait:   args.SendRetryWait,

		stackMux: &sync.Mutex{},
		stack:    make([]*retryItem, args.BufferSize),
		max:      int(args.BufferSize) - 1,
		top:      -1,
	}
	blip.Debug("buff %d, send timeout %s", rb.max+1, rb.sendTimeout)
	return rb
}

// Name returns the name of the real sink, not "retry".
func (rb *Retry) Name() string {
	return rb.sink.Name()
}

// Send buffers, sends, and retries sending metrics on failure. It is safe to call
// from multiple goroutines.
func (rb *Retry) Send(ctx context.Context, m *blip.Metrics) error {
	rb.push(&retryItem{metrics: m}) // top of stack

	rb.sendMux.Lock()
	if rb.sending {
		// Already sending; enqueue and return early. The active sender will
		// send the queued metrics for as long as ctx allows.
		rb.sendMux.Unlock()
		return nil
	}

	// ----------------------------------------------------------------------
	// Active sender
	rb.sending = true
	rb.sendMux.Unlock()

	defer func() {
		rb.sendMux.Lock()
		rb.sending = false
		rb.sendMux.Unlock()
	}()

	ctx2, cancel := context.WithTimeout(ctx, rb.sendTimeout)
	defer cancel()

	// Process stack from newest to oldest, while we have time
	n := 0
	for next := rb.pop(nil); next != nil; next = rb.pop(next) {
		// Stop when either context is cancelled
		select {
		case <-ctx2.Done():
			return nil
		default:
		}

		// Throttle between send, except on first send
		if n > 0 {
			time.Sleep(rb.retryWait)
		}
		n += 1

		// Send next oldest metrics
		var err error
		if sink, ok := rb.sink.(CheckpointSink); ok {
			next.checkpoint, err = sink.SendWithCheckpoint(ctx2, next.metrics, next.checkpoint)
		} else {
			err = rb.sink.Send(ctx2, next.metrics)
		}
		if err != nil {
			rb.event.Errorf(event.SINK_SEND_ERROR, "%s", err.Error())
			next = nil // don't pop metrics; retry stack from top down
		}
	}

	return nil
}

func (rb *Retry) push(item *retryItem) {
	rb.stackMux.Lock()
	defer rb.stackMux.Unlock()
	if rb.top < rb.max {
		rb.top++
	} else {
		// Push down stack (push off oldest metrics)
		copy(rb.stack, rb.stack[1:])
	}
	rb.stack[rb.top] = item
}

func (rb *Retry) pop(sent *retryItem) *retryItem {
	rb.stackMux.Lock()
	defer rb.stackMux.Unlock()

	// Remove sent metrics from the stack
	if sent != nil {
		if rb.stack[rb.top] == sent {
			// Easy case: sent is still on top, so just dereference to free memory
			rb.stack[rb.top] = nil
			rb.top--
		} else {
			// Metrics were on top but got pushed down, so remove metrics from
			// middle of stack
			k := -1 // index of sent in stack
			for i := range rb.stack {
				if rb.stack[i] != sent {
					continue
				}
				k = i // found sent in stack
				break
			}
			if k > -1 {
				copy(rb.stack[k:], rb.stack[k+1:]) // remove sent for stack
				rb.top--
			}
			// If k still equals -1, then sent was push off the stack,
			// so we can ignore
		}
	}

	// Stack empty? Nothing to pop.
	if rb.top == -1 {
		return nil
	}

	// Return next oldest metrics
	return rb.stack[rb.top]
}
