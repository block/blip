// Copyright 2024 Block, Inc.

package default_plan

import "github.com/cashapp/blip/v2"

func Exporter() blip.Plan {
	return blip.Plan{
		Name:   blip.DEFAULT_EXPORTER_PLAN,
		Source: "blip",
		Levels: map[string]blip.Level{
			"prom": blip.Level{ // key name and
				Name: "prom", // level Name must be equal
				Freq: "10s",  // not real but gives collectors a time limit
				Collect: map[string]blip.Domain{
					"status.global": {
						Name: "status.global",
						Options: map[string]string{
							"all": "yes",
						},
					},
					"var.global": {
						Name: "var.global",
						Options: map[string]string{
							"all": "yes",
						},
					},
					"innodb": {
						Name: "innodb",
						Options: map[string]string{
							"all": "enabled",
						},
					},
				},
			},
		},
	}
}
