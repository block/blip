// Copyright 2024 Block, Inc.

// Package monitor provides core Blip components that, together, monitor one
// database target. Most monitoring logic happens in the package, but package
// metrics is closely related: this latter actually collect metrics, but it
// is driven by this package. Other Blip packages are mostly set up and support
// of monitors.
package monitor

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/event"
	"github.com/cashapp/blip/ha"
	"github.com/cashapp/blip/heartbeat"
	"github.com/cashapp/blip/plan"
	"github.com/cashapp/blip/prom"
	"github.com/cashapp/blip/status"
)

// Monitor monitors one database target. The monitor is a high-level component
// that runs (and keeps running) four monitor subsystems:
//   - Plan changer (PCH)
//   - Level collector (LCO)
//   - Blip heartbeat writer
//   - Exporter (Prometheus)
//
// Each subsystem is optional based on the config, but LCO runs by default
// because it contains the Engine component that does actual metrics collection.
// If any subsystem crashes (returns for any reason or panics), the monitor
// stops and restarts all subsystems. The monitor doesn't stop until Stop is
// called. Consequently, if a monitor is not configured correctly (for example,
// it can't connect to the database), it tries and reports every forever.
//
// Monitors are loaded, created, and initially started only by the MonitorLoader.
// A monitor can be stopped and started (again) via the server API.
//
// A monitor is uniquely identified by its monitor ID, which should be included
// in all output by the monitor and its subsystems. The monitor ID is set when
// loaded by the MonitoLoad, which calls blip.MonitorId to determine the value.
//
// A monitor is completely self-contained and independent. For example, each monitor
// has its own LCO, engine, and metric collectors.
type Monitor struct {
	// Required to create; created in Loader.makeMonitor()
	monitorId       string
	cfg             blip.ConfigMonitor
	dbMaker         blip.DbFactory
	planLoader      *plan.Loader
	sinks           []blip.Sink
	transformMetric func([]*blip.Metrics) error

	// Core components
	runMux     *sync.RWMutex
	db         *sql.DB
	dbProvider blip.DbProvider
	dsn        string // redacted (no password)
	promAPI    *prom.API
	exporter   *Exporter
	lco        LevelCollector
	pch        PlanChanger
	hbw        *heartbeat.Writer

	// Control chans and sync
	runLoopChan chan struct{} // Stop(): stop the monitor
	runChan     chan struct{} // stop goroutines run by monitor
	wg          sync.WaitGroup

	event event.MonitorReceiver
	retry *backoff.ExponentialBackOff
	ha    ha.Manager
}

// MonitorArgs are required arguments to NewMonitor.
type MonitorArgs struct {
	Config          blip.ConfigMonitor
	DbMaker         blip.DbFactory
	PlanLoader      *plan.Loader
	Sinks           []blip.Sink
	TransformMetric func([]*blip.Metrics) error
	HA              ha.Manager
}

// NewMonitor creates a new Monitor with the given arguments. The caller must
// call Boot then, if that does not return an error, Run to start monitoring
// the database target.
func NewMonitor(args MonitorArgs) *Monitor {
	retry := backoff.NewExponentialBackOff()
	retry.MaxElapsedTime = 0
	retry.MaxInterval = 20 * time.Second
	return &Monitor{
		monitorId:       args.Config.MonitorId,
		cfg:             args.Config,
		dbMaker:         args.DbMaker,
		planLoader:      args.PlanLoader,
		sinks:           args.Sinks,
		transformMetric: args.TransformMetric,
		ha:              args.HA,
		// --
		runMux: &sync.RWMutex{},
		wg:     sync.WaitGroup{},
		event:  event.MonitorReceiver{MonitorId: args.Config.MonitorId},
		retry:  retry,
	}
}

// MonitorId returns the monitor ID.
func (m *Monitor) MonitorId() string {
	return m.monitorId
}

// Config returns the monitor config.
func (m *Monitor) Config() blip.ConfigMonitor {
	return m.cfg
}

// DSN returns the redacted DSN (no password).
func (m *Monitor) DSN() string {
	return m.dsn
}

// Stop stops the monitor. It is idempotent and thread-safe.
//
// Start/stop monitors only through the Loader. DO NOT call Start or
// Stop directly, else the running state of the monitor and the Loader
// will be out of sync.
func (m *Monitor) Stop() error {
	m.runMux.Lock()
	defer m.runMux.Unlock()

	blip.Debug("%s: Stop call", m.monitorId)
	defer blip.Debug("%s: Stop return", m.monitorId)

	// Stop runLoop() _first_, else it will restart run()
	select {
	case <-m.runLoopChan: // not running
		blip.Debug("%s: already stopped", m.monitorId)
		return nil
	default: // running
	}

	// Stop runLoop so it won't restart everything
	close(m.runLoopChan)

	// Stop and wait for monitor subsystems
	m.stop(false, "Stop")

	// Everything should be stopped now, so close database resources.
	m.closeDB()

	event.Sendf(event.MONITOR_STOPPED, "%s", m.monitorId)
	status.Monitor(m.monitorId, status.MONITOR, "stopped at %s", blip.FormatTime(time.Now()))
	return nil
}

// Start starts the monitor. If it's already running, it returns an error.
// It can be called again after calling Stop.
//
// Start/stop monitors only through the Loader. DO NOT call Start or
// Stop directly, else the running state of the monitor and the Loader
// will be out of sync.
func (m *Monitor) Start() error {
	m.runMux.Lock()
	defer m.runMux.Unlock()
	select {
	case <-m.runLoopChan:
		// not running
		blip.Debug("%s: start (again)", m.monitorId)
	default:
		if m.runLoopChan != nil { // running
			return fmt.Errorf("ready running")
		}
		// first start
		blip.Debug("%s: start (first)", m.monitorId)
	}
	m.runLoopChan = make(chan struct{})
	go m.runLoop()
	return nil
}

// runnLoop starts and keeps the monitor subsystems running by calling startup.
// If any subsystem crashes, it calls startup again. It doesn't stop until Stop
// is called.
//
// runLoop is called only by Start, which guards (serializes) it.
func (m *Monitor) runLoop() {
	defer blip.Debug("%s: runLoop return", m.monitorId)
	for {
		// New runChan for every iteration; it can only be used/closed once
		m.runMux.Lock()
		m.runChan = make(chan struct{})
		m.runMux.Unlock()

		// Run monitor startup sequence to start all (enabled) monitor subsystems.
		// If successful, the monitor is running but that does _not_ mean metrics
		// are collecting because collectors can fail for different reasons.
		err := m.startup()
		m.setErr(err, false)
		if err != nil {
			time.Sleep(m.retry.NextBackOff())
			continue
		}

		// Monitor is running. Wait for either Stop (which closes m.runLoopChan)
		// or one of the monitor subsystems to return/panic (which closes m.runChan).
		// On Stop, return immediately: user is stopping the monitor completely.
		// On m.runChan close (via stop func), we restart almost immediately because
		// Blip never stops trying to send metrics.
		m.retry.Reset()
		status.Monitor(m.monitorId, status.MONITOR, "running since %s", blip.FormatTime(time.Now()))
		select {
		case <-m.runLoopChan: // Stop called
			return
		case <-m.runChan: // internal failure
			blip.Debug("%s: runChan closed; restarting", m.monitorId)
			time.Sleep(1 * time.Second) // between monitor restarts
		}
	}
}

// startup starts the monitor subsystems, which are optional depending on config:
// heartbeat writer, exporter API (Prometheus emulation), PCH, and LCO.
// The monitor is running once these have started. If any subsystem crashes
// (or returns for any reason), it calls stop() to stop the other subsystems,
// then runLoop() calls startup again to restart monitoring.
//
// startup is called only by runLoop, which guards (serializes) and monitors it.
func (m *Monitor) startup() (err error) {
	blip.Debug("%s: startup call", m.monitorId)
	defer blip.Debug("%s: startup return", m.monitorId)

	// Catch panic in this func, pretty much just the DB-plan loop because
	// each monitor subsystems goroutine has its own defer/recover.
	defer func() {
		if r := recover(); r != nil {
			m.panic(r)
			m.stop(true, "startup") // stop monitor subsystems
		}
	}()

	// //////////////////////////////////////////////////////////////////////
	// DB-plan loop
	// //////////////////////////////////////////////////////////////////////

	// Release database resources from an earlier failed startup or subsystem
	// restart before creating the next monitor-owned connection set.
	m.runMux.Lock()
	m.closeDB()
	m.runMux.Unlock()

	// ----------------------------------------------------------------------
	// Make DSN and *sql.DB. This does NOT connect to the database.
	for {
		status.Monitor(m.monitorId, status.MONITOR, "making DB/DSN (not connecting)")
		dbProvider, db, dsnRedacted, err := m.makeDB()
		m.setErr(err, false)
		if err == nil { // success
			m.runMux.Lock()
			m.db = db
			m.dbProvider = dbProvider
			m.dsn = dsnRedacted
			status.Monitor(m.monitorId, status.MONITOR_DSN, "%s", dsnRedacted)
			m.runMux.Unlock()
			break
		}
		select {
		case <-m.runLoopChan:
			return nil // runLoop stopped (Stop called)
		default:
		}
		status.Monitor(m.monitorId, status.MONITOR, "error making DB/DSN, sleep and retry: %s", err)
		time.Sleep(m.retry.NextBackOff())
	}

	// ----------------------------------------------------------------------
	// Load monitor plans, if any. This MIGHT connect to the database if the plan
	// is stored in a table.
	for {
		status.Monitor(m.monitorId, status.MONITOR, "loading plans")
		err := m.planLoader.LoadMonitor(m.cfg, m.dbMaker)
		m.setErr(err, false)
		if err == nil { // success
			break
		}
		select {
		case <-m.runLoopChan:
		default:
			return nil // // runLoop stopped (Stop called)
		}
		status.Monitor(m.monitorId, status.MONITOR, "error loading plans, sleep and retry: %s", err)
		time.Sleep(m.retry.NextBackOff())
	}

	// //////////////////////////////////////////////////////////////////////
	// Start monitor subsystems
	// //////////////////////////////////////////////////////////////////////

	m.runMux.Lock()
	defer m.runMux.Unlock()
	defer func() {
		if err != nil {
			m.stop(false, "startup error")
		}
	}()

	// ----------------------------------------------------------------------
	// Heartbeat

	// Run optional heartbeat write. When enabled (by setting heartbeat.freq),
	// Blip writes millisecond-precision timestamps to a table that the repl.lag
	// metric collector uses to report sub-second replication lag.
	if m.cfg.Heartbeat.Freq != "" {
		status.Monitor(m.monitorId, status.MONITOR, "starting heartbeat")
		m.hbw = heartbeat.NewWriter(m.monitorId, m.db, m.cfg.Heartbeat)
		hbw := m.hbw
		runChan := m.runChan
		m.wg.Add(1)
		go func() {
			defer m.stopRun(runChan, "heartbeat.Writer") // stop monitor subsystems
			defer m.wg.Done()                            // notify stop()
			defer func() {                               // catch panic in heartbeat.Writer
				if r := recover(); r != nil {
					m.panic(r)
				}
			}()
			doneChan := make(chan struct{}) // Monitor uses wg
			hbw.Write(runChan, doneChan)
		}()
	}

	// ----------------------------------------------------------------------
	// Exporter API (Prometheus emulation)

	if m.cfg.Exporter.Mode != "" {
		status.Monitor(m.monitorId, status.MONITOR, "starting exporter")

		// Load the exporter plan. If the user specified config.exporter.plan,
		// that plan is loaded. Else, the default exporter plan will be loaded
		// because ConfigExporter.ApplyDefaults will have set the plan name to
		// the default, and the plan.Loader will have loaded it, too.
		promPlan, err := m.planLoader.Plan(m.monitorId, m.cfg.Exporter.Plan, nil)
		if err != nil {
			blip.Debug("%s: %s", m.monitorId, err.Error())
			status.Monitor(m.monitorId, "exporter", "not running: error loading plans: %s", err)
			return err
		}

		// An exporter plan can have only 1 level. This should really be checked
		// on boot, but since exporter is the exception, we accept this less than
		// ideal design.
		if len(promPlan.Levels) != 1 {
			err := fmt.Errorf("exporter plan has %d levels, expected 1", len(promPlan.Levels))
			blip.Debug("%s: %s", m.monitorId, err.Error())
			status.Monitor(m.monitorId, "exporter", "not running: invalid plan: %s", err)
			return err
		}

		// Run API to emulate an exporter, responding to GET /metrics
		m.exporter = NewExporter(m.cfg.Exporter, promPlan, newEngineWithDBProvider(m.cfg, m.db, m.dbProvider))
		m.promAPI = prom.NewAPI(
			m.cfg.Exporter,
			m.monitorId,
			m.exporter,
		)
		promAPI := m.promAPI
		runChan := m.runChan

		m.wg.Add(1)
		go func() {
			defer status.RemoveComponent(m.monitorId, "exporter")
			defer m.stopRun(runChan, "prom.API") // stop monitor subsystems
			defer m.wg.Done()                    // notify stop()
			defer func() {                       // catch panic in exporter API
				if r := recover(); r != nil {
					m.panic(r)
				}
			}()
			err := promAPI.Run()
			if err == nil { // shutdown
				blip.Debug("%s: prom api stopped", m.monitorId)
				return
			}
			blip.Debug("%s: prom api error: %s", m.monitorId, err.Error())
			status.Monitor(m.monitorId, "exporter", "API error (restart in 1s): %s", err)
		}()

		if m.cfg.Exporter.Mode == blip.EXPORTER_MODE_LEGACY {
			blip.Debug("%s: legacy mode", m.monitorId)
			status.Monitor(m.monitorId, status.MONITOR, "running in exporter legacy mode")
			m.event.Sendf(event.MONITOR_STARTED, "%s", m.dsn)
			return nil
		}
	}

	// ----------------------------------------------------------------------
	// Level collector (LCO)

	// Start the LCO before the PCH because the latter calls the former on
	// state change. The LCO starts paused (engine not running) until a plan
	// is set by calling lco.ChangePlan. Or, ff the PCH is enabled by
	// config.plans.change, then it will do this; if it's not enabled,
	// we'll do it as the last startup step.
	status.Monitor(m.monitorId, status.MONITOR, "starting level collector")
	m.lco = newLevelCollectorWithDBProvider(LevelCollectorArgs{
		Config:           m.cfg,
		DB:               m.db,
		PlanLoader:       m.planLoader,
		Sinks:            m.sinks,
		TransformMetrics: m.transformMetric,
	}, m.dbProvider)
	lco := m.lco
	runChan := m.runChan

	m.wg.Add(1)
	go func() {
		defer m.stopRun(runChan, "LCO") // stop monitor subsystems
		defer m.wg.Done()               // notify stop()
		defer func() {                  // catch panic in LCO
			if r := recover(); r != nil {
				m.panic(r)
			}
		}()
		doneChan := make(chan struct{}) // Monitor uses wg
		lco.Run(runChan, doneChan)
	}()

	// ----------------------------------------------------------------------
	// Plan changer (PCH)

	if m.cfg.Plans.Change.Enabled() {
		// Run option plan changer (PCH). When enabled, the PCH checks
		// the state of MySQL. If the state changes, it calls lco.ChangePlan
		// to change the plan as configured by config.monitors.plans.adjust.<state>.
		status.Monitor(m.monitorId, status.MONITOR, "starting plan changer")
		m.pch = NewPlanChanger(PlanChangerArgs{
			MonitorId: m.monitorId,
			Config:    m.cfg.Plans.Change,
			DB:        m.db,
			LCO:       m.lco,
			HA:        m.ha,
		})
		pch := m.pch

		m.wg.Add(1)
		go func() {
			defer m.stopRun(runChan, "PCH") // stop monitor subsystems
			defer m.wg.Done()               // notify stop()
			defer func() {                  // catch panic in PCH
				if r := recover(); r != nil {
					m.panic(r)
				}
			}()
			doneChan := make(chan struct{}) // Monitor uses wg
			pch.Run(runChan, doneChan)      // start LCO indirectly
		}()
	} else {
		// When the PCH is not enabled, we must init the state and plan,
		// which are ACTIVE and first (""), respectively. Since PCH is
		// optional, this is the normal case: startup presuming MySQL is
		// active and use the monitor's first plan.
		//
		// Do need retry or error handling because ChangePlan tries forever,
		// or until called again.
		status.Monitor(m.monitorId, status.MONITOR, "starting plan %s", m.cfg.Plan)
		m.lco.ChangePlan(blip.STATE_ACTIVE, m.cfg.Plan) // start LCO directly
	}

	m.event.Sendf(event.MONITOR_STARTED, "%s", m.dsn)
	return nil
}

func (m *Monitor) makeDB() (blip.DbProvider, *sql.DB, string, error) {
	providerFactory, ok := m.dbMaker.(blip.DbProviderFactory)
	if !ok {
		db, dsn, err := m.dbMaker.Make(m.cfg)
		return nil, db, dsn, err
	}

	provider, dsn, err := providerFactory.MakeProvider(m.cfg)
	if err != nil {
		if provider != nil {
			provider.Close()
		}
		return nil, nil, "", err
	}
	if provider == nil {
		return nil, nil, "", fmt.Errorf("database provider factory returned a nil provider")
	}
	db := provider.Primary()
	if db == nil {
		provider.Close()
		return nil, nil, "", fmt.Errorf("database provider returned a nil primary connection")
	}
	return provider, db, dsn, nil
}

// closeDB releases the current monitor-owned database resources. The caller
// must hold runMux.
func (m *Monitor) closeDB() {
	if m.dbProvider != nil {
		m.dbProvider.Close()
		m.dbProvider = nil
		m.db = nil
		return
	}
	if m.db != nil {
		m.db.Close()
		m.db = nil
	}
}

// stop stops the monitor subsystems started in startup. It does not stop the
// monitor; Stop does that. Stopping only the monitor subsystems causes runLoop
// to restart them.
func (m *Monitor) stop(lock bool, caller string) {
	if lock {
		m.runMux.Lock()
		defer m.runMux.Unlock()
	}

	// Already stopped?
	select {
	case <-m.runChan:
		blip.Debug("%s: stop called by %s (noop)", m.monitorId, caller)
		return // already stopped
	default:
		blip.Debug("%s: stop called by %s (first)", m.monitorId, caller)
		defer blip.Debug("%s: stop return for %s", m.monitorId, caller)
	}

	// Stop the monitor subsystem goroutines (except exporter/Prom API)
	close(m.runChan)

	// Stop exporter API; this one doesn't use stop/done control chans because
	// it's running an http.Server
	if m.promAPI != nil {
		m.promAPI.Stop()
		m.promAPI = nil
	}

	// Wait for monitor subsystem goroutines to return
	status.Monitor(m.monitorId, status.MONITOR, "stopping goroutines")
	m.wg.Wait()
	if m.exporter != nil {
		m.exporter.Stop()
		m.exporter = nil
	}

	// A subsystem failure restarts the whole monitor, including its database
	// provider. Close the current provider only after every subsystem has
	// stopped using its connections.
	m.closeDB()
}

// stopRun stops a subsystem generation only if it is still current. A
// subsystem from an earlier startup can finish after runLoop has installed a
// new run channel; it must not stop that newer generation.
func (m *Monitor) stopRun(runChan chan struct{}, caller string) {
	m.runMux.Lock()
	defer m.runMux.Unlock()
	if m.runChan != runChan {
		blip.Debug("%s: stop called by %s for obsolete run (noop)", m.monitorId, caller)
		return
	}
	m.stop(false, caller)
}

func (m *Monitor) setErr(err error, isPanic bool) {
	if err != nil {
		m.event.Error(event.MONITOR_ERROR, err.Error())
		status.Monitor(m.monitorId, "error:"+status.MONITOR, "error: %s", err)
	} else {
		status.RemoveComponent(m.monitorId, "error:"+status.MONITOR)
	}
}

func (m *Monitor) panic(r interface{}) {
	b := make([]byte, 4096)
	n := runtime.Stack(b, false)
	errMsg := fmt.Errorf("PANIC: %s: %s\n%s", m.monitorId, r, string(b[0:n]))
	m.setErr(errMsg, true)
}
