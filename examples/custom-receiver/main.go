package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/server"
)

// JSON implements a custom event.Receiver that converts Blip events to JSON
// and prints them to stdout.
type JSON struct{}

var _ event.Receiver = JSON{}

func (y JSON) Recv(e event.Event) {
	bytes, _ := json.Marshal(e)
	fmt.Println(string(bytes))
}

func main() {
	event.SetReceiver(JSON{}) // Must set receiver before calling Boot

	// Create, boot, and run the custom Blip server
	s := server.Server{}
	if err := s.Boot(server.Defaults()); err != nil {
		log.Fatalf("server.Boot failed: %s", err)
	}
	if err := s.Run(server.ControlChans()); err != nil { // blocking
		log.Fatalf("server.Run failed: %s", err)
	}
}
