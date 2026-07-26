// Copyright 2024 Block, Inc.

package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/event"
	"github.com/cashapp/blip/metrics"
	"github.com/cashapp/blip/status"
)

// CollectParallel sets how many domains to collect in parallel. Currently, this
// is not configurable via Blip config; it can only be changed via integration.
var CollectParallel = 2

// collection is the metrics from one domain. The flow is roughly:
//
//	Engine.collectionChan <- go cl.collect(<domain collector>)
//
// See clutch ("cl") at the bottom of this file. The embedded blip.Metrics
// is used for field Interval: if collection.Interval == the current interval
// (normal case), then it's metrics for the current Engine.Collect call.
// Else, collection.Interval < current interval because it's metrics from a
// long-running collector. In this case, the embedded blip.Metrics is used to
// recreate the blip.Metrics from the past.
type collection struct {
	blip.Metrics
	domain  string
	vals    []blip.MetricValue // don't name "values": conflicts with embedded blip.Metrics.Values
	err     error
	runtime time.Duration
}

// Engine runs domain metric collectors to collect metrics. It's called by the
// LevelCollector (LCO) at intervals and expected to collect and return within
// an engine make runtime (EMR) passed to Collect. The LCO creates the Engine.
// On LCO.Stop, the Engine must stop/destroy all collectors because the LCO will
// stop/destroy the Engine. Like all Monitor components, an Engine is not restarted
// or reused, it's recreated if the Monitor is restarted.
type Engine struct {
	cfg        blip.ConfigMonitor
	db         *sql.DB
	dbProvider blip.DbProvider
	monitorId  string
	// --
	event event.MonitorReceiver
	*sync.Mutex
	plan           blip.Plan
	collectors     map[string]*clutch   // keyed on domain
	collectAt      map[string][]*clutch // keyed on level, sorted ascending by CMR
	checkAt        map[string][]*clutch // keyed on level
	collectionChan chan collection
}

func NewEngine(cfg blip.ConfigMonitor, db *sql.DB) *Engine {
	return newEngineWithDBProvider(cfg, db, nil)
}

func newEngineWithDBProvider(cfg blip.ConfigMonitor, db *sql.DB, dbProvider blip.DbProvider) *Engine {
	return &Engine{
		cfg:        cfg,
		db:         db,
		dbProvider: dbProvider,
		monitorId:  cfg.MonitorId,
		// --
		event:          event.MonitorReceiver{MonitorId: cfg.MonitorId},
		Mutex:          &sync.Mutex{},
		collectors:     map[string]*clutch{},
		collectAt:      map[string][]*clutch{},
		checkAt:        map[string][]*clutch{},
		collectionChan: make(chan collection, len(metrics.List())*2),
	}
}

func (e *Engine) MonitorId() string {
	return e.monitorId
}

func (e *Engine) DB() *sql.DB {
	return e.db
}

// Prepare prepares the engine to collect metrics for the plan. The engine
// must be successfully prepared for Collect() to work because Prepare()
// initializes metric collectors for every level of the plan. Prepare() can
// be called again when, for example, the PlanChanger detects a state change
// and calls the LevelCollector to change plans, which than calls this func with
// the new state plan.
//
// Do not call this func concurrently! It does not guard against concurrent
// calls. Serialization is handled by the only caller: LevelCollector.ChangePlan().
func (e *Engine) Prepare(ctx context.Context, plan blip.Plan, before, after func()) error {
	blip.Debug("%s: prepare %s (%s)", e.monitorId, plan.Name, plan.Source)
	e.event.Sendf(event.ENGINE_PREPARE, "%s", plan.Name)
	status.Monitor(e.monitorId, status.ENGINE_PREPARE, "%s", plan.Name)
	defer status.RemoveComponent(e.monitorId, status.ENGINE_PREPARE)

	// Report last error, if any
	var lerr error
	defer func() {
		if lerr != nil {
			e.event.Error(event.ENGINE_PREPARE_ERROR, lerr.Error())
			status.Monitor(e.monitorId, "error:"+status.ENGINE_PREPARE, "%s", lerr.Error())
		} else {
			// success
			status.RemoveComponent(e.monitorId, "error:"+status.ENGINE_PREPARE)
		}
	}()

	// Connect to MySQL. DO NOT loop and retry; try once and return on error
	// to let the caller (a LevelCollector.changePlan goroutine) retry with backoff.
	status.Monitor(e.monitorId, status.ENGINE_PREPARE, "%s: connect to MySQL", plan.Name)
	dbctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := e.db.PingContext(dbctx)
	cancel()
	if err != nil {
		lerr = fmt.Errorf("while connecting to MySQL: %s", err)
		return lerr
	}

	// Find minimum intervals (freq) for plan and each domain.
	minFreq, domainFreq := plan.Freq()

	// Create and prepare metric collectors for every level. Return on error
	// because the error might be fatal, e.g. something misconfigured and the
	// plan cannot work.
	collectors := map[string]*clutch{}  // keyed on domain
	collectAt := map[string][]*clutch{} // keyed on level
	domainsAt := map[string][]string{}  // keyed on level
	allDomains := map[string]bool{}     // keyed on level
	for levelName, level := range plan.Levels {
		domains := make([]string, 0, len(level.Collect))
		domainsAt[levelName] = make([]string, 0, len(level.Collect))

		for domain := range level.Collect {
			// At this level, collect this domain (sorted by domain freq below)
			domains = append(domains, domain)
			allDomains[domain] = true

			// Make collector first time it's seen (they're unique in a plan)
			if _, ok := collectors[domain]; ok {
				continue // already seen
			}
			c, err := metrics.MakeWithDBProvider(
				domain,
				blip.CollectorFactoryArgs{
					Config:    e.cfg,
					DB:        e.db,
					MonitorId: e.monitorId,
				},
				e.dbProvider,
			)
			if err != nil {
				lerr = fmt.Errorf("while making %s collector: %s", domain, err)
				return lerr
			}

			status.Monitor(e.monitorId, status.ENGINE_PREPARE, "%s: prepare collector %s", plan.Name, domain)
			cleanup, err := c.Prepare(ctx, plan)
			if err != nil {
				lerr = fmt.Errorf("while preparing %s/%s/%s: %s", plan.Name, levelName, domain, err)
				return lerr
			}

			// Wrap collector in a clutch that provides the connection between
			// engine and collector: engaged during Engine.Collect; disengaged
			// if EMR (engine max runtime) expires but CMR (collector max runtime)
			// allows the collector to keep running.
			collectors[domain] = &clutch{ // new clutch
				c:              c,
				cleanup:        cleanup,
				domain:         domain,
				cmr:            blip.TimeLimit(0.2, domainFreq[domain], 2*time.Second), // collector interval minus 20% (max 2s)
				collectionChan: e.collectionChan,
				event:          e.event,
				Mutex:          &sync.Mutex{},
			}
		}

		// Sort domains collected at this level by freq (asc)
		sort.Slice(domains, func(i, j int) bool { return domainFreq[domains[i]] < domainFreq[domains[j]] })
		blip.Debug("domain priority at %s: %v", levelName, domains)
		collectAt[levelName] = make([]*clutch, len(domains))
		for i := range domains {
			collectAt[levelName][i] = collectors[domains[i]]
		}

		domainsAt[levelName] = domains // used outside loop below
	}

	// Inverse: at each level, which domains are NOT run and instead checked
	// for past long-running metrics
	for levelName, domains := range domainsAt {
		// Find domains NOT collected at this level. During Collect at this level,
		// these domains will be check for pending metrics to flush. That's why we
		// exclude domains collected at the level: collecting will flush pending
		// metrics, too.
		check := []*clutch{}
		included := []string{}
		for domain := range allDomains {
			// Is domain excluded because it's collect at this level, or a min freq domain?
			atLevel := false
			for _, excludedDomain := range domains {
				if domain == excludedDomain || domainFreq[domain] == minFreq {
					atLevel = true
					break
				}
			}
			if !atLevel { // domain NOT collected at this level, so check/flush at this level
				check = append(check, collectors[domain])
				included = append(included, domain)
			}
		}
		e.checkAt[levelName] = check // all domains to check at this level (none collected at this level)
		blip.Debug("check pending flush at %s: %v", levelName, included)
	}

	// Successfully prepared the plan
	status.Monitor(e.monitorId, status.ENGINE_PREPARE, "%s: level-collector before callback", plan.Name)
	before() // notify caller (lco.changePlan) that we're about to swap the plan

	status.Monitor(e.monitorId, status.ENGINE_PREPARE, "%s: finalize", plan.Name)
	e.Lock() // LOCK plan -------------------------------------------

	// Stop current collectors and call their cleanup func, if any. For example,
	// the repl collector uses a cleanup func to stop its heartbeat.BlipReader goroutine.
	e.stopCollectors()

	e.collectors = collectors // new mcs
	e.plan = plan             // new plan
	e.collectAt = collectAt   // new levels

	e.Unlock() // UNLOCK plan ---------------------------------------

	status.Monitor(e.monitorId, status.ENGINE_PLAN, "%s", plan.Name)
	e.event.Sendf(event.ENGINE_PREPARE_SUCCESS, "%s", plan.Name)

	status.Monitor(e.monitorId, status.ENGINE_PREPARE, "%s: level-collector after callback", plan.Name)
	after() // notify caller (lco.changePlan) that we have swapped the plan

	return nil
}

// Collect collects the metrics at the given level. There are 3 return guarantees
// for the slice of metrics:
//
//   - metrics[0] is non-nil (always returns at least one blip.Metrics)
//   - metrics[n].Values is non-nil (but might be empty, no values)
//   - []metrics is sorted ascending by Interval
//
// Collect returns when all collectors it starts return, or when emrCtx
// (engine max runtime) expires. The former is the normal case.
//
// Both metrics and an error can be returned in the case of partially success:
// some collectors work but others fail. Caller should check returned metrics
// even if an error is returned.
func (e *Engine) Collect(emrCtx context.Context, interval uint, levelName string, startTime time.Time) ([]*blip.Metrics, error) {
	// Don't change plans or stop while collecting. Engine max runtime (emrCtx)
	// ensures this collection won't take too long and block Prepare or Stop.
	e.Lock()
	defer e.Unlock()

	// Collection ID for status and logging, paired with monitor ID like "db1.local: myPlan/kpi/5"
	coId := fmt.Sprintf("%s/%s/%d", e.plan.Name, levelName, interval)

	// All metric collectors at this level
	domains, ok := e.collectAt[levelName]
	if !ok {
		panic(fmt.Sprintf("Engine.Collect called for interval %d level %s but plan has no domains at this level", interval, levelName))
	}
	if domains == nil {
		blip.Debug("Engine.Stop was called, dropping interval %d level %s", interval, levelName)
		return []*blip.Metrics{{Values: map[string][]blip.MetricValue{}}}, nil // see return guarantee in Collect comment
	}
	blip.Debug("%s: %s: collect", e.monitorId, coId)
	status.Monitor(e.monitorId, status.ENGINE_COLLECT, "%s", coId+": collecting")

	// Collect metrics for each domain in parallel (limit: CollectParallel)
	sem := make(chan bool, CollectParallel) // semaphore for CollectParallel
	for i := 0; i < CollectParallel; i++ {
		sem <- true
	}

	// Collect domains at this level
	m := &blip.Metrics{
		Plan:      e.plan.Name,
		Level:     levelName,
		Interval:  interval,
		MonitorId: e.monitorId,
		Begin:     startTime,
		// Don't set Values yet because these fields are copied in cl.collect
	}
	running := map[string]bool{}
	for _, cl := range domains {
		select {
		case <-sem:
			go cl.collect(*m, sem)
			running[cl.c.Domain()] = true
		case <-emrCtx.Done():
			blip.Debug("EMR timeout starting collectors")
			// @todo skip pending and sweep, goto end?
			break
		}
	}

	// Flush metrics from domains NOT started at this level
	for _, cl := range e.checkAt[levelName] {
		cl.Lock()
		if cl.pending {
			cl.flush(false)
		}
		cl.Unlock()
	}

	// Wait for all collectors to finish, then record end time
	m.Values = map[string][]blip.MetricValue{}
	metrics := []*blip.Metrics{m}
	errs := map[string]error{} // includes nil to clear "error:domain" status
	nValues := 0
SWEEP:
	for len(running) > 0 {
		status.Monitor(e.monitorId, status.ENGINE_COLLECT, "%s: receiving metrics, %d collectors running", coId, len(running))
		select {
		case c := <-e.collectionChan:
			/*
				DO NOT USE c.Values!
				That's the embedded collection.(blip.Metrics).Values.
				Use only c.vals.
			*/
			if c.Interval == interval { // this interval/collection
				delete(running, c.domain)
				if n := len(c.vals); n > 0 {
					metrics[0].Values[c.domain] = c.vals
					nValues += n
				}
				errs[c.domain] = c.err // save all, including nil
			} else { // past interval/collection
				// Merge with existing past interval metrics, else append new *blip.Metrics
				merged := false
				for _, m := range metrics {
					if m.Interval != c.Interval {
						continue
					}
					m.Values[c.domain] = c.vals
					merged = true
					break
				}
				if !merged {
					old := c.Metrics
					old.Values = map[string][]blip.MetricValue{c.domain: c.vals}
					metrics = append(metrics, &old)
				}
			}
			// @todo if c.runtime > some config, drop and send event.DROP_METRICS_RUNTIME
		case <-emrCtx.Done(): // engine runtime max
			blip.Debug("EMR timeout receiving collections")
			// @todo event so we can monitor is edge case
			break SWEEP
		}
	}
	metrics[0].End = time.Now()

	// Log collector errors and update collector status
	status.Monitor(e.monitorId, status.ENGINE_COLLECT, "%s", coId+": logging errors")
	errCount := 0
	for domain, err := range errs {
		switch err {
		case blip.ErrMore:
			status.RemoveComponent(e.monitorId, "error:"+domain)
			status.Monitor(e.monitorId, "background:"+domain, "at %s for %s", metrics[0].Begin, coId)
		case nil:
			status.RemoveComponent(e.monitorId, "error:"+domain)
			status.RemoveComponent(e.monitorId, "background:"+domain)
		default:
			errCount += 1
			errMsg := fmt.Sprintf("%s/%s: %s", coId, domain, err)
			status.Monitor(e.monitorId, "error:"+domain, "at %s: %s", metrics[0].Begin, errMsg)
			e.event.Error(event.COLLECTOR_ERROR, errMsg) // log by default
		}
	}

	status.Monitor(e.monitorId, status.ENGINE_COLLECT, "%s: done: %d started, %d domains %d values collected, %s runtime, %d error",
		coId, len(domains), len(domains)-len(running), nValues, metrics[0].End.Sub(metrics[0].Begin), errCount)

	// Sort intervals in ascending order so sinks receive domains in order.
	// This is a special case but possible when, for example, domain X runs
	// every 3rd interval and uses ErrMore to report progressively:
	//   interval 1: start, report, bg (ErrMore,keep running)
	//   interval 2: report more and stop
	//   interval 3: flush ^ and start again
	// In this case, metrics has X@1 and X@3 and must be sorted in metrics
	// in that order so a sink processing X gets them in order. Since clutch
	// serializes per-domain, sorting by interval is sufficient because there
	// cannot be two X@N for the same N (unless there's a bug in the clutch).
	if len(metrics) > 1 {
		sort.Slice(metrics, func(i, j int) bool { return metrics[i].Interval < metrics[j].Interval })
	}

	// Total success? Yes if no errors.
	if errCount == 0 {
		return metrics, nil
	}

	// Partial success? Yes if there are some metrics values.
	if nValues > 0 {
		return metrics, fmt.Errorf("%s: partial success: %d metrics collected, %d errors", coId, nValues, errCount)
	}

	// Errors and zero metrics: all collectors failed
	return metrics, fmt.Errorf("%s: failed: zero metrics collected, %d errors", coId, errCount)
}

// Stop the engine and cleanup any metrics associated with it.
// TODO: There is a possible race condition when this is called. Since
// Engine.Collect is called as a go-routine, we could have an invocation
// of the function block waiting for Engine.Stop to unlock
// after which Collect would run after cleanup has been called.
// This could result in a panic, though that should be caught and logged.
// Since the monitor is stopping anyway this isn't a huge issue.
func (e *Engine) Stop() {
	blip.Debug("Engine.Stop called")
	e.Lock()
	defer e.Unlock()
	e.stopCollectors()
	// Prevent Collect from running in case it's blocked on mutex
	for level := range e.collectAt {
		e.collectAt[level] = nil
	}
}

func (e *Engine) stopCollectors() {
	/* -- CALLER MUST LOCK Engine -- */
	for _, cl := range e.collectors {
		cl.Lock()
		if !cl.running {
			cl.Unlock()
			blip.Debug("%s: %s not running", e.monitorId, cl.c.Domain())
			continue
		}
		blip.Debug("%s: %s stopping", e.monitorId, cl.c.Domain())
		cl.cancel()
		if cl.cleanup != nil {
			blip.Debug("%s: %s cleanup", e.monitorId, cl.c.Domain())
			cl.cleanup()
		}
		cl.Unlock()
	}
}

// --------------------------------------------------------------------------

// A clutch connects the engine to one domain metric collector. It's 1-to-1:
// one domain metrics collector to one clutch ("cl"). Each one is created in
// Engine.Prepare and run as a goroutine in Engine.Collect. The clutch is
// primarily responsible for ensuring its collector runs only in serial.
// The first if-block of cl.run checks this: if an earlier cl.run is still
// running, it's a collector fault and the earlier cl.run is fenced off
// (metrics will be dropped) in case it ever returns. Second responsibility
// is letting the collector run without blocking Engine.Collect because some
// domains are long-running but the engine has to collect in serial, so it
// can't wait for long-running collectors.
//
// Follow the link in DESIGN.md to learn more about this component.
type clutch struct {
	c              blip.Collector
	cleanup        func()            // from c.Prepare (optional)
	domain         string            // c.Domain
	cmr            time.Duration     // collector max runtime (CMR)
	collectionChan chan<- collection // flush vals/err to
	event          event.MonitorReceiver
	*sync.Mutex

	// When running:
	m         blip.Metrics
	ctx       context.Context // context.WithTimeout(cmr)
	cancel    context.CancelFunc
	running   bool               // collect() running
	bg        bool               // true if c.Collect returns ErrMore
	pending   bool               // vals ready to flush
	vals      []blip.MetricValue // pending values from c
	err       error              // last error from c
	startTime time.Time          // collector runtime
	stopTime  time.Time          // collector runtime
	fence     uint               // set on collect fault (see below)
}

func (cl *clutch) collect(m blip.Metrics, sem chan bool) {
	cl.Lock() // ___LOCK___

	// ----------------------------------------------------------------------
	// Check that previous collection is done and flushed

	if cl.running {
		// Collector fault: it didn't terminate itself at CMR
		cl.event.Errorf(event.COLLECTOR_FAULT, "%s: metrics from interval %d will be dropped if the collector recovers: %+v", cl.domain, cl.m.Interval, cl)
		cl.fence = m.Interval
		if cl.cancel != nil {
			cl.cancel()
		}
	}

	// Background collection finished after last interval sweep and now
	// the domain is schedule to run at this interval, which is fine because
	// collector did stop running within its CMR (checked above ^), so we
	// just flush the last metrics (if any) then start again.
	if cl.pending {
		blip.Debug("flushing last values from background %s", cl.domain)
		cl.flush(false) // false -> don't override bg stop time (see defer below)
	}

	// ----------------------------------------------------------------------
	// Start new collection. DO NOT defer and set cl.running = false before
	// here else the last values flush case above ^ will set cl.running = false
	// when it returns, which is not the case: this func is only done running
	// when the code below returns.
	cl.startTime = time.Now() // real start time, not interval start time
	cl.m = m                  // copy blip.Metrics fields
	cl.running = true

	// Collector max runtime (CMR) is interval start time + cmr because this
	// collector might have been started after some delay in Engine.Collect
	// but it complete
	cl.ctx, cl.cancel = context.WithDeadline(context.Background(), m.Begin.Add(cl.cmr))

	// Local interval for this run/goroutine. If the collector has a bug such that
	// it doesn't return within its CMR and Engine.Collect runs this domain again,
	// then the very first if-block in this function will fence off this interval.
	// If/when the faulty Collect finally returns, "if interval < cl.fence" checks
	// will cause the faulty Collect metrics to be dropped. This var must be local
	// and it must be check before changing any cl.* fields because the goroutine
	// that detects the fault will write over the cl.* fields with the new interval.
	interval := m.Interval
	cancel := cl.cancel

	cl.Unlock() // ___unlock___

	defer func() {
		if r := recover(); r != nil {
			if !cl.bg { // foreground
				select {
				case sem <- true:
				default:
				}
			}
			b := make([]byte, 4096)
			n := runtime.Stack(b, false)
			perr := fmt.Errorf("PANIC: monitor ID %s: %s: %v\n%s", cl.m.MonitorId, cl.domain, r, string(b[0:n]))
			cl.event.Error(event.COLLECTOR_PANIC, perr.Error())
		}
		cancel() // local cancel, not cl.cancel, in case goroutine is behind the fence
		cl.Lock()
		if interval < cl.fence {
			cl.event.Errorf(event.DROP_METRICS_FENCE, "%s: dropping metrics because this interval %d < fence %d due to collector fault", cl.domain, interval, cl.fence)
			cl.Unlock()
			return // this goroutine is behind the fence, do NOT modify cl fields
		}
		cl.running = false
		if cl.bg {
			cl.stopTime = time.Now() // bg stop time
			blip.Debug("background %s done: runtime=%s pending=%t err=%v",
				cl.domain, cl.stopTime.Sub(cl.startTime), cl.pending, cl.err)
		}
		cl.Unlock()
	}()

	// ----------------------------------------------------------------------
	// FAST PATH: foreground collect once, unblock next collector (sem), and
	// flush metrics back to engine for reporting, the probably done
	vals, err := cl.c.Collect(cl.ctx, cl.m.Level)
	sem <- true
	cl.Lock()
	if interval < cl.fence {
		cl.Unlock()
		return // this goroutine is behind the fence, do NOT modify cl fields
	}
	cl.vals = vals
	cl.err = err
	cl.flush(err != blip.ErrMore)
	cl.Unlock()
	if err != blip.ErrMore {
		return
	}

	// ----------------------------------------------------------------------
	// Special case: blip.ErrMore == long-running collector, keep running and
	// saving metrics in cl.vals until collector stops returning blip.ErrMore.
	// The engine will call cl.flush every interval if cl.pending is true.
	cl.Lock()
	cl.bg = true
	cl.Unlock()
	for err == blip.ErrMore && err != context.Canceled && err != context.DeadlineExceeded {
		vals, err = cl.c.Collect(nil, "") // background collect
		cl.Lock()
		if interval < cl.fence {
			cl.Unlock()
			return // this goroutine is behind the fence, do NOT modify cl fields
		}
		if len(vals) > 0 {
			cl.pending = true
			cl.vals = append(cl.vals, vals...)
		}
		cl.err = err
		cl.Unlock()
	}
}

func (cl *clutch) flush(done bool) {
	/* -- CALLER MUST LOCK clutch -- */
	c := collection{
		Metrics: cl.m, // embedded blip.Metrics fields
		domain:  cl.domain,
		vals:    cl.vals,
		err:     cl.err,
	}
	if done {
		cl.stopTime = time.Now()
	}
	if !cl.stopTime.IsZero() {
		c.End = cl.stopTime
		c.runtime = cl.stopTime.Sub(cl.startTime)
	}

	// Flush metrics back to engine BEFORE resetting them below
	select {
	case cl.collectionChan <- c:
	default:
		cl.event.Errorf(event.DROP_METRICS_FLUSH, "%s: dropping metrics interval %d because channel blocked", cl.domain, c.Interval)
	}

	cl.pending = false
	cl.vals = []blip.MetricValue{}
	cl.err = nil
}
