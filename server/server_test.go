// Copyright 2024 Block, Inc.

package server_test

import (
	"testing"

	"github.com/cashapp/blip/v2/server"
)

func TestServerBootDefault(t *testing.T) {
	// Server should boot successfully with defaults
	s := server.Server{}

	env, plugins, factories := server.Defaults()
	env.Args = []string{} // remove go test command line args

	err := s.Boot(env, plugins, factories)
	if err != nil {
		t.Error(err)
	}
}

func TestServerBootCheck(t *testing.T) {
	// With --run=false, it's a "boot check": start up but don't actually run.
	// This makes Run() return immediately when called because startup sequence
	// is Boot() then Run() in bin/blip/main.go.
	s := server.Server{}
	env, plugins, factories := server.Defaults()
	env.Args = []string{"--run=false"}

	err := s.Boot(env, plugins, factories)
	if err != nil {
		t.Error(err)
	}

	err = s.Run(server.ControlChans())
	if err != nil {
		t.Error(err)
	}
}
