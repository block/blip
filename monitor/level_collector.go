// Copyright 2024 Block, Inc.

package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/event"
	"github.com/cashapp/blip/plan"
	"github.com/cashapp/blip/status"
)

// LevelCollector (LCO) executes the current plan to collect metrics.
// It's also responsible for changing the plan when called by the PlanChanger.
//
// The term "collector" is a little misleading because the LCO doesn't collect
// metrics, but it is the first step in the metrics collection process, which
// looks roughly like: LCO -> Engine -> metric collectors -> MySQL.
// In Run, the LCO checks every 1s for the highest level in the plan to collect.
// For example, after 5s it'll collect levels with a frequency divisible by 5s.
// See https://block.github.io/blip/plans/file/.
//
// Metrics from MySQL flow back to the LCO as blip.Metrics, which the LCO
// passes to blip.Plugin.TransformMetrics if specified, then to all sinks
// specified for the monitor.
type LevelCollector interface {
	// Run runs the collector to collect metrics; it's a blocking call.
	Run(stopChan, doneChan chan struct{}) error

	// ChangePlan changes the plan; it's called by the PlanChanger.
	ChangePlan(newState, newPlanName string) error

	// Pause pauses metrics collection until ChangePlan is called.
	Pause()
}

var _ LevelCollector = &lco{}

// lco is the implementation of LevelCollector.
type lco struct {
	cfg              blip.ConfigMonitor
	planLoader       *plan.Loader
	sinks            []blip.Sink
	transformMetrics func([]*blip.Metrics) error
	// --
	monitorId   string
	engine      *Engine
	emr         time.Duration        // engine max runtime = levels[0].Freq
	metricsChan chan []*blip.Metrics // sorted ascending by Interval
	event       event.MonitorReceiver

	stateMux *sync.Mutex
	state    string
	plan     blip.Plan
	levels   []plan.SortedLevel
	paused   bool

	changeMux            *sync.Mutex
	changePlanCancelFunc context.CancelFunc
	changePlanDoneChan   chan struct{}
	stopped              bool
}

type LevelCollectorArgs struct {
	Config           blip.ConfigMonitor
	DB               *sql.DB
	PlanLoader       *plan.Loader
	Sinks            []blip.Sink
	TransformMetrics func([]*blip.Metrics) error
}

func NewLevelCollector(args LevelCollectorArgs) *lco {
	return &lco{
		cfg:              args.Config,
		planLoader:       args.PlanLoader,
		sinks:            args.Sinks,
		transformMetrics: args.TransformMetrics,
		// --
		monitorId:   args.Config.MonitorId,
		engine:      NewEngine(args.Config, args.DB),
		stateMux:    &sync.Mutex{},
		paused:      true,
		changeMux:   &sync.Mutex{},
		event:       event.MonitorReceiver{MonitorId: args.Config.MonitorId},
		metricsChan: make(chan []*blip.Metrics, 10),
	}
}

// TickerDuration sets the internal ticker duration for testing. This is only
// called for testing; do not called outside testing.
func TickerDuration(d, e time.Duration) {
	tickerMux.Lock() // make go test -race happy
	tickerDuration = d
	timeElapsed = e
	tickerMux.Unlock()
}

// Normally Blip runs 1s to 1s: tick every 1s (duration) = 1s elapsed. But for
// testing we speed up both. For example, 10ms/1s makes Run tick every 10 ms but
// act like 1s has elapsed. This is for test plans with realistic whole-second
// durations. The RBB tests use 100ms/100ms because that test plan is design for
// 100ms intervals, so it can run through 5 intervals in about 500ms.
var (
	tickerMux      = &sync.Mutex{}   // make go test -race happy
	tickerDuration = 1 * time.Second // used for testing
	timeElapsed    = 1 * time.Second // used for testing
)

// recvMetrics receives metrics on metricsChan and send them to all the sinks.
// This is a goroutine run by keepRecvMetrics and restarted by keepRecvMetrics
// if a sink (or the transformMetrics) panics. It stops when stopSinksChan is
// closed in Run, and it closes doneChan when stopped.
func (c *lco) recvMetrics(stopSinksChan, doneChan chan struct{}) {
	defer func() {
		close(doneChan)
		if r := recover(); r != nil {
			b := make([]byte, 4096)
			n := runtime.Stack(b, false)
			perr := fmt.Errorf("PANIC: sinks: %s: %v\n%s", c.monitorId, r, string(b[0:n]))
			c.event.Error(event.LCO_RECEIVER_PANIC, perr.Error())
		}
	}()
RECV:
	for {
		status.Monitor(c.monitorId, status.LEVEL_SINKS, "idle")
		select {
		case <-stopSinksChan:
			return
		case metrics := <-c.metricsChan:
			if c.transformMetrics != nil {
				blip.Debug("%s: transform metrics", c.monitorId)
				status.Monitor(c.monitorId, status.LEVEL_SINKS, "TransformMetrics")
				if err := c.transformMetrics(metrics); err != nil {
					blip.Debug("%s: transform metrics error, dropping metrics: %v", c.monitorId, err)
					continue RECV
				}
			}
			for _, m := range metrics {
				coId := fmt.Sprintf("%s/%s/%d", m.Plan, m.Level, m.Interval)
				for _, sink := range c.sinks {
					sinkName := sink.Name()
					status.Monitor(c.monitorId, status.LEVEL_SINKS, "%s", coId+": sending to "+sinkName)
					err := sink.Send(context.Background(), m) // @todo ctx with timeout
					if err != nil {
						c.event.Errorf(event.SINK_SEND_ERROR, "%s :%s", sinkName, err) // log by default
						status.Monitor(c.monitorId, "error:"+sinkName, "%s", err.Error())
					} else {
						status.RemoveComponent(c.monitorId, "error:"+sinkName)
					}
				}
			}
		}
	}
}

// keepRecvMetrics keeps a recvMetrics goroutine running. If a sink or the
// transformMetrics plugin panic, it must be restarted to keep metrics flowing.
func (c *lco) keepRecvMetrics(stopSinksChan chan struct{}) {
	for {
		doneChan := make(chan struct{})
		go c.recvMetrics(stopSinksChan, doneChan)
		select {
		case <-stopSinksChan:
			return
		case <-doneChan:
			// recvMetrics goroutine stopped but why? Did it return because
			// stopSinksChan, or because of a panic?
			select {
			case <-stopSinksChan:
				return // stopSinksChan closed
			default:
				// Probably a sink panic
			}
		}
	}
}

func (c *lco) Run(stopChan, doneChan chan struct{}) error {
	defer close(doneChan)

	// Keep receiving metrics for as long as the LCO is running
	stopSinksChan := make(chan struct{})
	go c.keepRecvMetrics(stopSinksChan)
	defer close(stopSinksChan)

	// -----------------------------------------------------------------------
	// LCO main loop: collect metrics every configured minimum level interval

	status.Monitor(c.monitorId, status.LEVEL_COLLECTOR, "started at %s (paused until plan change)", blip.FormatTime(time.Now()))
	tickerMux.Lock() // make go test -race happy
	td, te := tickerDuration, timeElapsed
	tickerMux.Unlock()
	s := -1 * te // -1 so first tick=0 and all levels collected
	interval := uint(0)
	ticker := time.NewTicker(td)
	defer ticker.Stop()
	for startTime := range ticker.C {
		s = s + te

		// Was monitor stopped?
		select {
		case _, ok := <-stopChan: // yes, return immediately
			// Stop changePlan goroutine (if any) and prevent new ones in the
			// pathological case that the LCH calls ChangePlan while the LCO
			// is terminating
			if ok {
				break
			}
			blip.Debug("stopChan closed at %s s=%d interval=%d", startTime, s, interval)
			c.changeMux.Lock()
			defer c.changeMux.Unlock()
			c.stopped = true // make ChangePlan do nothing
			select {
			case <-c.changePlanDoneChan:
				c.changePlanCancelFunc() // stop --> changePlan goroutine
				<-c.changePlanDoneChan   // wait for changePlan goroutine
			default:
			}
			c.engine.Stop() // stop all collectors and run their cleanup func
			return nil
		default: // no
		}

		// Paused because no plan is set or it's being changed?
		c.stateMux.Lock() // -- LOCK --
		if c.paused {
			s = -1 * timeElapsed
			interval = 0
			c.stateMux.Unlock() // -- Unlock
			continue
		}

		// Determine lowest level to collect
		level := -1
		for i := range c.levels {
			if s%c.levels[i].Freq == 0 {
				level = i
			}
		}
		if level == -1 {
			c.stateMux.Unlock() // -- Unlock
			continue            // no metrics to collect at this frequency
		}

		// Collect metrics at this level
		interval += 1
		c.collect(interval, c.levels[level].Name, startTime)

		c.stateMux.Unlock() // -- UNLOCK --
	}
	return nil
}

func (c *lco) collect(interval uint, levelName string, startTime time.Time) {
	status.Monitor(c.monitorId, status.LEVEL_COLLECT, "%s/%s: collecting", c.plan.Name, levelName)
	defer func() {
		if err := recover(); err != nil { // catch panic in engine and TransformMetrics
			b := make([]byte, 4096)
			n := runtime.Stack(b, false)
			c.event.Errorf(event.LCO_COLLECT_PANIC, "PANIC: %s: %s\n%s", c.monitorId, err, string(b[0:n]))
		}
		status.Monitor(c.monitorId, status.LEVEL_COLLECT, "%s/%s/%d: collected in %s", c.plan.Name, levelName, interval, time.Now().Sub(startTime))
	}()

	// **************************************************************
	// COLLECT METRICS
	//
	// Collect all metrics at this level. This is where metrics
	// collection begins. Then Engine.Collect does the real work.
	emrCtx, emrCancel := context.WithDeadline(context.Background(), startTime.Add(c.emr))
	defer emrCancel()
	metrics, err := c.engine.Collect(emrCtx, interval, levelName, startTime)
	blip.Debug("%s: level %s: done in %s", c.monitorId, levelName, metrics[0].End.Sub(metrics[0].Begin))

	if err != nil {
		status.Monitor(c.monitorId, "error:collect", "%s", err.Error())
		c.event.Error(event.ENGINE_COLLECT_ERROR, err.Error())
	} else {
		status.RemoveComponent(c.monitorId, "error:collect")
	}

	status.Monitor(c.monitorId, status.LEVEL_COLLECT, "%s/%s: sending", c.plan.Name, levelName)
	select {
	case c.metricsChan <- metrics:
	default:
		c.event.Errorf(event.LCO_METRICS_FAULT, "metrics channel blocked (check for sink errors), dropping metrics: %s", metrics)
	}
}

// ChangePlan changes the metrics collect plan based on database state.
// It loads the plan from the plan.Loader, then it calls Engine.Prepare.
// This is the only time and place that Engine.Prepare is called.
//
// The caller is either LevelAdjuster.CheckState or Monitor.Start. The former
// is the case when config.monitors.plans.adjust is set. In this case,
// the LevelAdjuster (LPA) periodically checks database state and calls this
// function when the database state changes. It trusts that this function
// changes the state, so the LPA does not retry the call. The latter case,
// called from Monitor.Start, happen when the LPA is not enabled, so the
// monitor sets state=active, plan=<default>; then it trusts this function
// to keep retrying.
//
// ChangePlan is safe to call by multiple goroutines because it serializes
// plan changes, and the last plan wins. For example, if plan change 1 is in
// progress, plan change 2 cancels it and is applied. If plan change 3 happens
// while plan change 2 is in progress, then 3 cancels 2 and 3 is applied.
// Since the LPA is the only periodic caller and it has delays (so plans don't
// change too quickly), this shouldn't happen.
//
// Currently, the only way this function fails is if the plan cannot be loaded.
// That shouldn't happen because plans are loaded on startup, but it might
// happen in the future if Blip adds support for reloading plans via the API.
// Then, plans and config.monitors.*.plans.adjust might become out of sync.
// In this hypothetical error case, the plan change fails but the current plan
// continues to work.
func (c *lco) ChangePlan(newState, newPlanName string) error {
	// Serialize access to this func
	c.changeMux.Lock()
	defer c.changeMux.Unlock()

	if c.stopped { // Run stopped?
		return nil
	}

	// Check if changePlan goroutine from previous call is running
	select {
	case <-c.changePlanDoneChan:
	default:
		if c.changePlanCancelFunc != nil {
			blip.Debug("cancel previous changePlan")
			c.changePlanCancelFunc() // stop --> changePlan goroutine
			<-c.changePlanDoneChan   // wait for changePlan goroutine
		}
	}

	blip.Debug("start new changePlan: %s %s", newState, newPlanName)
	ctx, cancel := context.WithCancel(context.Background())
	c.changePlanCancelFunc = cancel
	c.changePlanDoneChan = make(chan struct{})

	// Don't block caller. If state changes again, LPA will call this
	// func again, in which case the code above will cancel the current
	// changePlan goroutine (if it's still running) and re-change/re-prepare
	// the plan for the latest state.
	go c.changePlan(ctx, c.changePlanDoneChan, newState, newPlanName)

	return nil
}

// changePlan is a gorountine run by ChangePlan It's potentially long-running
// because it waits for Engine.Prepare. If that function returns an error
// (e.g. MySQL is offline), then this function retires forever, or until canceled
// by either another call to ChangePlan or Run is stopped (LCO is terminated).
//
// Never all this function directly; it's only called via ChangePlan, which
// serializes access and guarantees only one changePlan goroutine at a time.
func (c *lco) changePlan(ctx context.Context, doneChan chan struct{}, newState, newPlanName string) {
	defer close(doneChan)

	c.stateMux.Lock()
	oldState := c.state
	oldPlanName := c.plan.Name
	c.stateMux.Unlock()
	change := fmt.Sprintf("state:%s plan:%s -> state:%s plan:%s", oldState, oldPlanName, newState, newPlanName)
	c.event.Sendf(event.CHANGE_PLAN, "%s", change)

	// Load new plan from plan loader, which contains all plans. Try forever because
	// that's what this func/gouroutine does: try forever (caller's expect that).
	// This shouldn't fail given that plans were already loaded and validated on startup,
	// but maybe plans reloaded after startup and something broke. User can fix by
	// reloading plans again.
	var newPlan blip.Plan
	var err error
	for {
		status.Monitor(c.monitorId, status.LEVEL_CHANGE_PLAN, "loading new plan %s (state %s)", newPlanName, newState)
		newPlan, err = c.planLoader.Plan(c.engine.MonitorId(), newPlanName, c.engine.DB())
		if err == nil {
			break // success
		}

		errMsg := fmt.Sprintf("%s: error loading new plan %s: %s (retrying)", change, newPlanName, err)
		status.Monitor(c.monitorId, status.LEVEL_CHANGE_PLAN, "%s", errMsg)
		c.event.Sendf(event.CHANGE_PLAN_ERROR, "%s", errMsg)
		time.Sleep(2 * time.Second)
	}

	change = fmt.Sprintf("state:%s plan:%s -> state:%s plan:%s", oldState, oldPlanName, newState, newPlan.Name)

	newPlan.MonitorId = c.monitorId
	newPlan.InterpolateEnvVars()
	newPlan.InterpolateMonitor(&c.cfg)

	// Convert plan levels to sorted levels for efficient level calculation in Run;
	// see code comments on sortedLevels.
	levels := plan.Sort(&newPlan)

	// ----------------------------------------------------------------------
	// Prepare the (new) plan
	//
	// This is two-phase commit:
	//   0. LCO: pause Run loop
	//   1. Engine: commit new plan
	//   2. LCO: commit new plan
	//   3. LCO: resume Run loop
	// Below in call c.engine.Prepare(ctx, newPlan, c.Pause, after), Prepare
	// does its work and, if successful, calls c.Pause, which is step 0;
	// then Prepare does step 1, which won't be collected yet because it
	// just paused LCO.Run which drives metrics collection; then Prepare calls
	// the after func/calleck defined below, which is step 2 and signals to
	// this func that we commit the new plan and resume Run (step 3) to begin
	// collecting that plan.

	after := func() {
		c.stateMux.Lock() // -- X lock --
		c.state = newState
		c.plan = newPlan
		c.levels = levels
		if len(levels) > 0 { // there can be 0 levels, e.g. plan/default.None
			c.emr = blip.TimeLimit(0.1, levels[0].Freq, time.Second) // interval minus 10% (max 1s)
		}

		// Changing state/plan always resumes (if paused); in fact, it's the
		// only way to resume after Pause is called
		c.paused = false
		status.Monitor(c.monitorId, status.LEVEL_STATE, "%s", newState)
		status.Monitor(c.monitorId, status.LEVEL_PLAN, "%s", newPlan.Name)
		status.Monitor(c.monitorId, status.LEVEL_COLLECTOR, "running since %s", blip.FormatTime(time.Now()))
		blip.Debug("%s: resume", c.monitorId)

		c.stateMux.Unlock() // -- X unlock --
	}

	// Try forever, or until context is cancelled, because it could be that MySQL is
	// temporarily offline. In the real world, this is not uncommon: Blip might be
	// started before MySQL, for example. We're running in a goroutine from ChangePlan
	// that already returned to its caller, so we're not blocking anything here.
	// More importantly, as documented in several place: this is _the code_ that
	// all other code relies on to try "forever" because a plan must be prepared
	// before anything can be collected.
	status.Monitor(c.monitorId, status.LEVEL_CHANGE_PLAN, "preparing new plan %s (state %s)", newPlan.Name, newState)
	retry := backoff.NewExponentialBackOff()
	retry.MaxElapsedTime = 0
	for {
		// ctx controls the goroutine, which might run "forever" if plans don't
		// change. ctxPrep is a timeout for Prepare to ensure that it does not
		// run try "forever". If preparing takes too long, there's probably some
		// issue, so we need to sleep and retry.
		ctxPrep, cancelPrep := context.WithTimeout(ctx, 10*time.Second)
		err := c.engine.Prepare(ctxPrep, newPlan, c.Pause, after)
		cancelPrep()
		if err == nil {
			break // success
		}
		if ctx.Err() != nil {
			blip.Debug("changePlan canceled")
			return // changePlan goroutine has been cancelled
		}
		status.Monitor(c.monitorId, status.LEVEL_CHANGE_PLAN, "%s: error preparing new plan %s: %s (retrying)", change, newPlan.Name, err)
		time.Sleep(retry.NextBackOff())
	}

	status.RemoveComponent(c.monitorId, status.LEVEL_CHANGE_PLAN)
	c.event.Sendf(event.CHANGE_PLAN_SUCCESS, "%s", change)
}

// Pause pauses metrics collection until ChangePlan is called. Run still runs,
// but it doesn't collect when paused. The only way to resume after pausing is
// to call ChangePlan again.
func (c *lco) Pause() {
	c.stateMux.Lock()
	c.paused = true
	status.Monitor(c.monitorId, status.LEVEL_COLLECTOR, "paused at %s", blip.FormatTime(time.Now()))
	c.event.Send(event.LCO_PAUSED)
	c.stateMux.Unlock()
}
