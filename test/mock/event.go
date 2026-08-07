// Copyright 2024 Block, Inc.

package mock

import (
	"github.com/cashapp/blip/v2/event"
)

type EventReceiver struct {
	RecvFunc func(event.Event)
}

var _ event.Receiver = EventReceiver{}

func (r EventReceiver) Recv(e event.Event) {
	if r.RecvFunc != nil {
		r.RecvFunc(e)
	}
}
