// Copyright 2024 Block, Inc.

package monitor

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/ha"
	"github.com/cashapp/blip/v2/status"
)

var Now func() time.Time = time.Now

// PlanChanger (PCH) changes the plan based on database instance state.
// If the plan changes, the PCH calls the LevelCollector (LCO) to do the
// real low-level work of swapping plans, because the LCO executes plans.
// In this sense, "Changer" is a bit misleading because it doesn't change
// the plan, it just determines if/when the plan should change, and then
// tells the LCO to actually change the plan.
type PlanChanger interface {
	Run(stopChan, doneChan chan struct{}) error
}

type PlanChangerArgs struct {
	MonitorId string
	Config    blip.ConfigPlanChange
	DB        *sql.DB
	LCO       LevelCollector
	HA        ha.Manager
}

var _ PlanChanger = &planChanger{}

type state struct {
	state string
	plan  string
	ts    time.Time
}

type change struct {
	after time.Duration
	plan  string
}

// planChanger is the implementation of PlanChanger.
type planChanger struct {
	cfg       blip.ConfigPlanChange
	monitorId string
	db        *sql.DB
	lco       LevelCollector
	ha        ha.Manager
	// --
	*sync.Mutex
	states  map[string]change
	prev    state
	curr    state
	pending state
	first   bool
	event   event.MonitorReceiver
	retry   *backoff.ExponentialBackOff
	lerr    error
}

func NewPlanChanger(args PlanChangerArgs) *planChanger {
	states := map[string]change{}
	d, _ := time.ParseDuration(args.Config.Offline.After)
	states[blip.STATE_OFFLINE] = change{
		after: d,
		plan:  args.Config.Offline.Plan,
	}
	d, _ = time.ParseDuration(args.Config.Standby.After)
	states[blip.STATE_STANDBY] = change{
		after: d,
		plan:  args.Config.Standby.Plan,
	}
	d, _ = time.ParseDuration(args.Config.ReadOnly.After)
	states[blip.STATE_READ_ONLY] = change{
		after: d,
		plan:  args.Config.ReadOnly.Plan,
	}
	d, _ = time.ParseDuration(args.Config.Active.After)
	states[blip.STATE_ACTIVE] = change{
		after: d,
		plan:  args.Config.Active.Plan,
	}

	retry := backoff.NewExponentialBackOff()
	retry.MaxElapsedTime = 0

	return &planChanger{
		monitorId: args.MonitorId,
		cfg:       args.Config,
		db:        args.DB,
		lco:       args.LCO,
		ha:        args.HA,
		// --
		Mutex:   &sync.Mutex{},
		states:  states,
		prev:    state{},
		curr:    state{state: blip.STATE_OFFLINE},
		pending: state{},
		first:   true,
		event:   event.MonitorReceiver{MonitorId: args.MonitorId},
		retry:   retry,
	}
}

// setErr sets the last internal error reported by Status.
func (pch *planChanger) setErr(err error) {
	if err != nil {
		status.Monitor(pch.monitorId, "error:"+status.PLAN_CHANGER, "%s", err.Error())
	} else {
		status.RemoveComponent(pch.monitorId, "error:"+status.PLAN_CHANGER)
	}
}

// Run calls CheckState every second; or, if offline, it uses an exponential
// backoff up to 60 seconds until back online (MySQL connection ok). There is
// no logic in this function; it's just a timed loop to call CheckState. It's
// run as a goroutine from Monitor.Run only if config.monitors.plans.adjust
// is enabled (blip.ConfigPlanChange.Enabled returns true).
func (pch *planChanger) Run(stopChan, doneChan chan struct{}) error {
	defer close(doneChan)
	defer status.Monitor(pch.monitorId, status.PLAN_CHANGER, "not running")

	status.Monitor(pch.monitorId, status.PLAN_CHANGER, "running")

	for {
		select {
		case <-stopChan:
			return nil
		default:
		}

		pch.CheckState()

		if pch.curr.state != blip.STATE_OFFLINE {
			time.Sleep(1 * time.Second)
		} else {
			time.Sleep(pch.retry.NextBackOff())
		}
	}
}

// CheckState checks the current state and changes state when necessary.
// This is the main logic, called periodically by Run.
func (pch *planChanger) CheckState() {
	now := Now()
	obsv := pch.state()

	pch.Lock()
	defer pch.Unlock()

	defer func() {
		status.Monitor(pch.monitorId, status.PLAN_CHANGER_STATE, "%s %s %s", pch.curr.state, pch.curr.plan, pch.curr.ts.Format(time.RFC3339))
		if pch.pending.state != "" {
			status.Monitor(pch.monitorId, status.PLAN_CHANGER_PENDING, "%s %s %s", pch.pending.state, pch.pending.plan, pch.pending.ts.Format(time.RFC3339))
		} else {
			status.RemoveComponent(pch.monitorId, status.PLAN_CHANGER_PENDING)
		}
	}()

	if obsv == pch.curr.state {
		// Our state == MySQL state, which is the normal good case.
		// But if we were waiting to change state, then reset.
		if !pch.pending.ts.IsZero() {
			pch.pending.ts = time.Time{}
			pch.pending.state = blip.STATE_NONE
			pch.event.Sendf(event.STATE_CHANGE_ABORT, "%s", obsv)
		}
	} else if obsv == pch.pending.state {
		// Still in the pending state; is it time to change?
		if now.Sub(pch.pending.ts) < pch.states[pch.pending.state].after {
			return // no, keep waiting
		}

		// Change state: current -> pending
		if err := pch.lcoChangePlan(pch.pending.state, pch.pending.plan); err != nil {
			pch.setErr(err)
			blip.Debug(err.Error())
			return // ok to ignore error; see comments on lcoChangePlan
		}
		pch.prev = pch.curr
		pch.curr = pch.pending
		pch.pending.ts = time.Time{}
		pch.pending.state = blip.STATE_NONE
		blip.Debug("%s: PCH state changed to %s", pch.monitorId, obsv)
		pch.event.Sendf(event.STATE_CHANGE_END, "%s", obsv)
	} else if pch.first && pch.curr.state == blip.STATE_OFFLINE {
		// On boot, we usually go from no state to online immediately, which is normal.
		// So set the plan for whatever state we've booted into.
		pch.first = false
		if err := pch.lcoChangePlan(obsv, pch.states[obsv].plan); err != nil {
			pch.setErr(err)
			blip.Debug(err.Error())
			return // ok to ignore error; see comments on lcoChangePlan
		}
		pch.prev = pch.curr
		pch.curr = state{
			state: obsv,
			ts:    now,
		}
		blip.Debug("%s: PCH start in state %s", pch.monitorId, obsv)
		pch.event.Sendf(event.STATE_CHANGE_END, "%s", obsv)
	} else {
		// State has changed, so start the timer: if this new state remains
		// for long enough, we'll change the plan to match (in the first else-if
		// block above).
		pch.pending.state = obsv
		pch.pending.ts = now
		pch.pending.plan = pch.states[obsv].plan
		blip.Debug("%s: PCH state changed to %s, waiting %s", pch.monitorId, obsv, pch.states[obsv].after)
		pch.event.Sendf(event.STATE_CHANGE_BEGIN, "%s", obsv)
		pch.retry.Reset()
	}
}

// lcoChangePlan calls LevelCollector.ChangePlan to change the metrics collection plan.
// Or, it calls LevelCollector.Pause if there is no plan, which is the usual case when
// offline (can't connect to MySQL. We presume that these calls do not fail; see
// LevelCollector.ChangePlan for details.
func (pch *planChanger) lcoChangePlan(state, planName string) error {
	status.Monitor(pch.monitorId, status.PLAN_CHANGER, "calling LevelCollector.ChangePlan: %s %s", state, planName)
	if planName == "" {
		pch.lco.Pause()
		return nil
	}
	return pch.lco.ChangePlan(state, planName)
}

const readOnlyQuery = "SELECT @@read_only, @@super_read_only"

// state queries MySQL to ascertain the HA and read-only state.
func (pch *planChanger) state() string {
	status.Monitor(pch.monitorId, status.PLAN_CHANGER, "checking HA standby")
	if pch.ha.Standby() {
		return blip.STATE_STANDBY
	}

	// Active, but is MySQL read-only?
	status.Monitor(pch.monitorId, status.PLAN_CHANGER, "checking MySQL read-only")

	var ro, sro int
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := pch.db.QueryRowContext(ctx, readOnlyQuery).Scan(&ro, &sro)
	cancel()
	pch.setErr(err)
	if err != nil {
		blip.Debug(err.Error())
		return blip.STATE_OFFLINE
	}

	if ro == 1 {
		return blip.STATE_READ_ONLY
	}

	return blip.STATE_ACTIVE
}
