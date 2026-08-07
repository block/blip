// Copyright 2024 Block, Inc.

package main

import (
	"log"

	"github.com/cashapp/blip/v2/server"
)

func main() {
	s := server.Server{}

	if err := s.Boot(server.Defaults()); err != nil {
		log.Fatalf("server.Boot failed: %s", err)
	}

	if err := s.Run(server.ControlChans()); err != nil {
		log.Fatalf("server.Run failed: %s", err)
	}
}
