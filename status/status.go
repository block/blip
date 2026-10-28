// Copyright 2024 Block, Inc.

// Package status provides real-time instantaneous status of every Blip component.
// The only caller is server.API via GET /status.
package status

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type status struct {
	*sync.Mutex
	blip     map[string]string
	monitors map[string]map[string]string
	counters map[string]map[string]*atomic.Uint64
}

var s = &status{
	Mutex:    &sync.Mutex{},
	blip:     map[string]string{},
	monitors: map[string]map[string]string{},         // monitorId => component
	counters: map[string]map[string]*atomic.Uint64{}, // monitorId => counter
}

// Reset resets everything to zero values. It's only used for test.
func Reset() {
	s = &status{
		Mutex:    &sync.Mutex{},
		blip:     map[string]string{},
		monitors: map[string]map[string]string{},         // monitorId => component
		counters: map[string]map[string]*atomic.Uint64{}, // monitorId => counter
	}
}

func Blip(component, msg string, args ...interface{}) {
	s.Lock()
	s.blip[component] = fmt.Sprintf(msg, args...)
	s.Unlock()
}

func Monitor(monitorId, component string, msg string, args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.monitors[monitorId]; !ok {
		s.monitors[monitorId] = map[string]string{}
	}
	s.monitors[monitorId][component] = fmt.Sprintf(msg, args...)
}

func RemoveComponent(monitorId, component string) {
	s.Lock()
	m, ok := s.monitors[monitorId]
	if ok {
		delete(m, component)
	}
	s.Unlock()
}

func ReportBlip() map[string]string {
	s.Lock()
	defer s.Unlock()
	status := map[string]string{}
	for k, v := range s.blip {
		status[k] = v
	}
	return status
}

func ReportMonitors(ids ...string) map[string]map[string]string {
	s.Lock()
	defer s.Unlock()
	var allow map[string]bool
	if len(ids) > 0 {
		allow = map[string]bool{}
		for _, id := range ids {
			allow[id] = true
		}
	}
	status := map[string]map[string]string{}
	for monitorId := range s.monitors {
		if len(allow) > 0 && !allow[monitorId] {
			continue
		}
		status[monitorId] = map[string]string{}
		for k, v := range s.monitors[monitorId] {
			status[monitorId][k] = v
		}
	}
	return status
}

func RemoveMonitor(monitorId string) {
	s.Lock()
	delete(s.monitors, monitorId)
	delete(s.counters, monitorId)
	s.Unlock()
}
