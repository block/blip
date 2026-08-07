// Copyright 2024 Block, Inc.

package plan

import (
	"sort"
	"time"

	"github.com/cashapp/blip/v2"
)

// SortedLevel represents a sorted level created by sortedLevels below.
type SortedLevel struct {
	Freq time.Duration
	Name string
}

// Sort levels ascending by frequency.
type byFreq []SortedLevel

func (a byFreq) Len() int           { return len(a) }
func (a byFreq) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFreq) Less(i, j int) bool { return a[i].Freq < a[j].Freq }

// Sort returns a list of levels sorted (asc) by frequency. Sorted levels
// are used in the main Run loop: for i := range c.levels. Sorted levels are
// required because plan levels are unorded because the plan is a map. We could
// check every level in the plan, but that's wasteful. With sorted levels, we
// can precisely check which levels to collect at every 1s tick.
//
// Also, plan levels are abbreviated whereas sorted levels are complete.
// For example, a plan says "collect X every 5s, and collect Y every 10s".
// But the complete version of that is "collect X every 5s, and collect X + Y
// every 10s." See "metric inheritance" in the docs.
//
// Also, we convert duration strings from the plan level to integers for sorted
// levels in order to do modulo (%) in the main Run loop.
func Sort(p *blip.Plan) []SortedLevel {
	// Make a sorted level for each plan level
	levels := make([]SortedLevel, len(p.Levels))
	i := 0
	for _, l := range p.Levels {
		d, _ := time.ParseDuration(l.Freq) // "5s" -> 5 (for freq below)
		levels[i] = SortedLevel{
			Name: l.Name,
			Freq: d,
		}
		i++
	}

	// Sort levels by ascending frequency
	sort.Sort(byFreq(levels))
	blip.Debug("%s levels: %v", p.Name, levels)

	//
	// Level	Freq	Level (=) includes (+)
	// 1		5		+ + + +
	// 2		20		+ +   =
	// 3		30		+ + =
	// 4		60		+ =
	// 5		300		=

	// "Low level, high frequency"

	for hi := len(levels) - 1; hi > 0; hi-- {
		higher := p.Levels[levels[hi].Name]

		for lo := hi - 1; lo >= 0; lo-- {

			// Skip if lower.Freq not multiple of high.Freq
			if levels[hi].Freq%levels[lo].Freq != 0 {
				continue
			}

			lower := p.Levels[levels[lo].Name]

			for domain := range lower.Collect {
				higherDomain, ok := higher.Collect[domain]
				if !ok {
					higherDomain = blip.Domain{
						Name:    domain,
						Metrics: []string{},
						Options: map[string]string{},
						Errors:  map[string]string{},
					}
				}
				higherDomain.Metrics = append(higherDomain.Metrics, lower.Collect[domain].Metrics...)
				for k, v := range lower.Collect[domain].Options {
					if _, ok := higherDomain.Options[k]; !ok {
						higherDomain.Options[k] = v
					}
				}
				for k, v := range lower.Collect[domain].Errors {
					if _, ok := higherDomain.Errors[k]; !ok {
						higherDomain.Errors[k] = v
					}
				}
				higher.Collect[domain] = higherDomain
			}
		}
	}

	return levels
}
