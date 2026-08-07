// Copyright 2024 Block, Inc.

package ha

import (
	"sync"

	"github.com/cashapp/blip/v2"
)

type Manager interface {
	Standby() bool
}

type ManagerFactory interface {
	Make(monitor blip.ConfigMonitor) (Manager, error)
}

type disabled struct {
}

func (d disabled) Standby() bool {
	return false
}

var once sync.Once
var Disabled = disabled{}

// Internal factory disabled by default (HA not used).
var f ManagerFactory = Disabled

func Register(mf ManagerFactory) {
	once.Do(func() {
		f = mf
		blip.Debug("register HA")
	})
}

func Make(args blip.ConfigMonitor) (Manager, error) {
	return f.Make(args)
}

func (d disabled) Make(_ blip.ConfigMonitor) (Manager, error) {
	return Disabled, nil
}
