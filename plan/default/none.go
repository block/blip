// Copyright 2024 Block, Inc.

package default_plan

import "github.com/cashapp/blip/v2"

// None returns an empty plan usual for standby instances of Blip.
func None() blip.Plan {
	return blip.Plan{
		Name:   "default-none",
		Source: "blip",
		Levels: map[string]blip.Level{},
	}
}
