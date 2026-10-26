// Copyright 2024 Block, Inc.

package monitor

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/ha"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/sink"
	"github.com/cashapp/blip/v2/status"
)

// LoadFunc is a callback that matches blip.Plugin.LoadMonitors.
// It's an arg to NewLoader, if specified by the user.
type LoadFunc func(blip.Config) ([]blip.ConfigMonitor, error)

// StartMonitorFunc is a callback that matches blip.Plugin.StartMonitor.
type StartMonitorFunc func(blip.ConfigMonitor) bool

var (
	ErrMonitorNotLoaded = errors.New("monitor not loaded")
	ErrStopLoss         = errors.New("stop-loss prevents reloading")
	ErrStopTimeout      = errors.New("monitor did not stop before the stop timeout; it is still stopping")
)

// loadedMonitor represents one validated and loaded Monitor created by
// a call to Load. started is false until StartMonitors or Start is called.
// Call Stop (or Unload) to stop a monitor.
//
// Start/stop monitors only through the Loader. DO NOT call Monitor.Start or
// Monitor.Stop directly, else the running state of the monitor and the Loader
// will be out of sync.
type loadedMonitor struct {
	monitor *Monitor
	started bool

	// startPending is true if the monitor was started while a monitor with
	// the same ID was still stopping. It starts when that stop finishes.
	startPending bool
}

// stoppingMonitor is a monitor that the Loader is stopping in the background,
// outside the Loader mutex, in runStop. The Loader mutex guards the
// Loader.stopping map that holds it, but not its fields: monitor and begin are
// set before the stop begins and never change, and done and finished are
// channels that runStop closes and waiters read without the mutex. Whether the
// stop timed out is local to runStop.
type stoppingMonitor struct {
	monitor  *Monitor
	begin    time.Time
	done     chan struct{} // closed when the monitor has stopped
	finished chan struct{} // closed when stopped has run: status removed, pending start started
}

// stopReportFactor sets how often a monitor that is stuck stopping is reported
// again after the first report: every stopReportFactor x stop timeout.
const stopReportFactor = 10

// StartResult is the result of starting a monitor with TryStart. In v3,
// TryStart will be folded into Start, which will return (StartResult, error).
type StartResult int

const (
	START_OK       StartResult = iota // monitor started
	START_PENDING                     // monitor with the same ID is still stopping; monitor starts when that stop finishes
	START_RUNNING                     // monitor was already running
	START_DECLINED                    // Plugins.StartMonitor returned false, so the monitor was not started
	START_ERROR                       // monitor was not started because of the returned error
)

// Loader is the singleton monitor loader and repo. It's created by the server
// and only used there (and via API calls). It's dynamic so monitors can be
// loaded (created) and unloaded (destroyed) while Blip is running, but the
// normal case is one load and start on Blip startup: Server.Boot calls Load,
// then Server.Run calls StartMonitors. The user can make API calls to reload
// while Blip is running.
//
// Loader is safe for concurrent use, but it's currently only called by the Server.
type Loader struct {
	cfg        blip.Config
	factory    blip.Factories
	plugin     blip.Plugins
	planLoader *plan.Loader
	// --
	repo            map[string]*loadedMonitor // keyed on monitorId
	stopLossPercent float64
	stopLossNumber  uint
	*sync.Mutex

	// stopping has one entry per monitor ID that is being stopped. The entry
	// is removed when Monitor.Stop returns. While an ID has an entry, a new
	// or restarted monitor with the same ID is not started (startPending)
	// so that two monitors with the same ID never run at the same time.
	stopping    map[string]*stoppingMonitor
	stopTimeout time.Duration

	stopChan     chan struct{}
	doneChan     chan struct{}
	rdsLoader    aws.RDSLoader
	startMonitor func(blip.ConfigMonitor) bool
}

type LoaderArgs struct {
	Config     blip.Config
	Factories  blip.Factories
	Plugins    blip.Plugins
	PlanLoader *plan.Loader
	RDSLoader  aws.RDSLoader
}

// NewLoader creates a new Loader singleton. It's called in Server.Boot and Server.Run.
func NewLoader(args LoaderArgs) *Loader {
	startMonitor := args.Plugins.StartMonitor
	if startMonitor == nil {
		startMonitor = func(blip.ConfigMonitor) bool { return true }
	}
	stopLossNumber, stopLossPercent, _ := blip.StopLoss(args.Config.MonitorLoader.StopLoss) // already validated
	stopTimeout, err := time.ParseDuration(args.Config.MonitorLoader.StopTimeout)
	if err != nil || stopTimeout <= 0 {
		stopTimeout, _ = time.ParseDuration(blip.DEFAULT_MONITOR_LOADER_STOP_TIMEOUT)
	}
	return &Loader{
		cfg:        args.Config,
		factory:    args.Factories,
		plugin:     args.Plugins,
		planLoader: args.PlanLoader,
		rdsLoader:  args.RDSLoader,
		// --
		stopLossPercent: stopLossPercent,
		stopLossNumber:  stopLossNumber,
		repo:            map[string]*loadedMonitor{},
		Mutex:           &sync.Mutex{},
		stopping:        map[string]*stoppingMonitor{},
		stopTimeout:     stopTimeout,
		stopChan:        make(chan struct{}),
		doneChan:        make(chan struct{}),
		startMonitor:    startMonitor,
	}
}

// StartMonitors starts all monitors that have been loaded but not started.
// This should be called after Load. On Blip startup, the server calls Load
// in Server.Boot, then StartMonitors in server.Run. The user can reload
// by calling the server API: /monitors/reload.
//
// This function is safe for concurrent use, but calls are serialized.
func (ml *Loader) StartMonitors() {
	ml.Lock()
	defer ml.Unlock()

	event.Send(event.MONITORS_STARTING)
	defer event.Send(event.MONITORS_STARTED)

	// Wait between starting each monitor to distribute CPU and network load.
	// Starting a monitor is quick (run a goroutine), so without a wait between
	// each, all the monitors would wake up, collect, and send metrics at
	// roughly the same time. A simple wait more evenly distributes the loads.
	n := 0
	for i := range ml.repo {
		if !ml.repo[i].started && !ml.repo[i].startPending {
			n += 1
		}
	}
	w := time.Duration(wait(n)) * time.Millisecond
	blip.Debug("%d to start, wait %s between each", n, w)

	// Start monitors that aren't started yet
	for i := range ml.repo {
		if ml.repo[i].started || ml.repo[i].startPending {
			continue // skip started monitors and monitors waiting to start
		}

		m := ml.repo[i] // m is *loadedMonitor
		status.Blip("monitor-loader", "starting %s", m.monitor.MonitorId())
		if _, err := ml.start(m); err != nil {
			// @todo event
			blip.Debug(err.Error()) // shouldn't happen
		}
		time.Sleep(w)
	}
	status.Blip("monitor-loader", "%d monitors started at %s", n, blip.FormatTime(time.Now()))
}

// start starts one loaded monitored. It's called by StartMonitors, Start, and
// stopped. The caller must hold the Loader mutex and ensure that the monitor
// is not already started.
//
// If a monitor with the same ID is still stopping, the monitor is not started
// now. It's marked startPending, and stopped starts it when that stop finishes.
func (ml *Loader) start(m *loadedMonitor) (StartResult, error) {
	monitorId := m.monitor.MonitorId()
	if sm, ok := ml.stopping[monitorId]; ok {
		blip.Debug("%s start pending: stopping since %s", monitorId, sm.begin)
		m.startPending = true
		status.Monitor(monitorId, status.MONITOR, "start pending: waiting for stop that began at %s", blip.FormatTime(sm.begin))
		return START_PENDING, nil
	}
	m.startPending = false

	// Call StartMonitor callback. Default allows all monitors to start,
	// but user might have provided callback to filter monitors.
	if !ml.startMonitor(m.monitor.Config()) {
		blip.Debug("%s not run", monitorId)
		return START_DECLINED, nil
	}

	// Start the MySQL monitor, which starts metrics collection
	if err := m.monitor.Start(); err != nil {
		return START_ERROR, err
	}
	m.started = true
	return START_OK, nil
}

// [ 1, 10) = <1s startup
// [10, 50] = ~1s
// (50,inf) = >1s + 1s per 50 [e.g. 100=2s, 200=4s]
const (
	min_wait = 20
	max_wait = 100
)

func wait(n int) int {
	if n <= 1 {
		return 0
	}
	ms := 1000 / n
	if ms < min_wait {
		return min_wait
	}
	if ms > max_wait {
		return max_wait
	}
	return ms
}

// Load loads all configured monitors and unloads (stops and removes) monitors
// that have been removed or changed since the last call to Load. It does not
// start new monitors. Call StartMonitors after Load to start new (or previously
// stopped) monitors.
//
// Server.Boot calls Load, then Server.Run calls StartMonitors.
//
// Load checks for stop-loss and does local MySQL auto-detection, if these two
// features are enabled.
//
// If Load returns error, the currently loaded monitors are not affected.
// The error indicates a problem loading monitors or a validation error.
// The exception is ctx: see below.
//
// Monitors that are unloaded are stopped in parallel and outside the Loader
// mutex. Load waits for them to stop, but no longer than the stop timeout
// (config.monitor-loader.stop-timeout) or ctx. A monitor that hasn't stopped
// by then is reported (event MONITOR_STOP_TIMEOUT and monitor status) and keeps
// stopping in the background; Load does not return an error for it. If ctx
// ends while Load waits, Load returns ctx.Err(), but the loaded monitors have
// already been updated: the new monitors are loaded and the removed monitors
// keep stopping in the background.
//
// This function is safe for concurrent use. Changes to the loaded monitors
// are serialized, but waiting for monitors to stop is not.
func (ml *Loader) Load(ctx context.Context) error {
	event.Send(event.MONITORS_LOADING)
	defer event.Send(event.MONITORS_LOADED)

	stops, err := ml.loadAndUnload(ctx)
	if err != nil {
		return err
	}
	if err := ml.waitStopped(ctx, stops, false); err != nil && err != ErrStopTimeout {
		return err // ctx ended; the repo is already updated
	}
	return nil
}

// loadAndUnload is the locked part of Load. It updates the repo and returns
// the stops of the monitors that it unloaded, which the caller waits for
// after the mutex is released.
func (ml *Loader) loadAndUnload(ctx context.Context) ([]*stoppingMonitor, error) {
	ml.Lock()
	defer ml.Unlock()

	defer func() {
		status.Blip("monitors", "%d", len(ml.repo))
	}()

	// ----------------------------------------------------------------------
	// Load

	// Low-level monitor loading returns a diff: new, chagned, and removed
	// monitors as compared to what's currently in the repo.
	diff, err := ml.load(ctx)
	if err != nil {
		return nil, err
	}

	// ----------------------------------------------------------------------
	// Stop-loss

	// Check config.monitor-loader.stop-loss: don't change monitors if there's
	// a big drop in the number because it might be a false-positive that will
	// set off alarms when a bunch of metrics fail to report.
	nBefore := float64(len(ml.repo))
	nNow := float64(len(diff.removed))
	if nNow < nBefore {
		var errMsg string
		if ml.stopLossPercent > 0 {
			lost := (nBefore - nNow) / nBefore
			if lost > ml.stopLossPercent {
				errMsg = fmt.Sprintf("before: %d; now: %d; lost %f%% > limit %f%%", int(nBefore), int(nNow), lost, ml.stopLossPercent)
			}
		}
		if ml.stopLossNumber > 0 {
			lost := uint(nBefore - nNow)
			if lost > ml.stopLossNumber {
				errMsg = fmt.Sprintf("before: %d; now: %d; lost %d > limit %d", int(nBefore), int(nNow), lost, ml.stopLossNumber)
			}
		}
		if errMsg != "" {
			event.Error(event.MONITORS_STOPLOSS, errMsg)
			return nil, ErrStopLoss
		}
	}

	// ----------------------------------------------------------------------
	// Update repo

	// Unload monitors that have been removed or changed. Changed monitors have
	// a new *Monitor in diff.added with the same monitor ID, so we must unload
	// the old monitor first, then add the monitor (with the same ID). The old
	// monitor might still be stopping when StartMonitors is called; if so, the
	// new monitor starts when the old one has stopped. See start.
	stops := []*stoppingMonitor{}
	for _, mon := range diff.removed {
		if sm := ml.unload(mon.MonitorId()); sm != nil {
			stops = append(stops, sm)
		}
	}
	for _, mon := range diff.changed {
		if sm := ml.unload(mon.MonitorId()); sm != nil {
			stops = append(stops, sm)
		}
	}

	// Add new monitors to the repo but don't start them: that's done in StartMonitors.
	for _, mon := range diff.added {
		ml.repo[mon.MonitorId()] = &loadedMonitor{
			monitor: mon,
			started: false,
		}
	}

	return stops, nil
}

type diff struct {
	added   []*Monitor
	removed []*Monitor
	changed []*Monitor
}

func (ml *Loader) load(ctx context.Context) (diff, error) {
	/*
		DO NOT MODIFY repo IN THIS FUNC.
		This func has no side effects on error.
		Only Load modifies repo.
	*/

	// diff is always returned, but currently the caller (Load) ignores it if
	// any error is returned. If we've already made new monitors, that's wasted
	// allocation, but we'll keep it for now in case Load ever supports partial
	// success. Plus, failure is the exception, not the normal, so wasted alloc
	// shouldn't be an issue.
	diff := diff{
		added:   []*Monitor{},
		removed: []*Monitor{},
		changed: []*Monitor{},
	}
	defer func() {
		last := fmt.Sprintf("added: %d removed: %d changed: %d",
			len(diff.added), len(diff.removed), len(diff.changed))
		status.Blip("monitor-loader", "%s on %s", last, blip.FormatTime(time.Now()))
	}()

	// All valid monitor configs loaded, keyed by monitor ID. See save().
	validConfigs := map[string]blip.ConfigMonitor{}

	// Judicious use of goto below requires no new vars come into scope
	var newConfigs []blip.ConfigMonitor
	var err error

	// ----------------------------------------------------------------------
	// LoadMonitors (overrides default load sequence)

	if ml.plugin.LoadMonitors != nil {
		blip.Debug("call plugin.LoadMonitors")
		status.Blip("monitor-loader", "loading from plugin")
		newConfigs, err = ml.plugin.LoadMonitors(ml.cfg)
		if err != nil {
			return diff, err
		}
		if err := ml.save(newConfigs, validConfigs); err != nil {
			return diff, err
		}
		goto MAKE_MONITORS // idiomatic Go if used judiciously
	}

	// -------------------------------------------------------------------
	// Default load sequence: config file, monitor files, AWS, local

	// First, monitors from the config file
	if len(ml.cfg.Monitors) != 0 {
		if err := ml.save(ml.cfg.Monitors, validConfigs); err != nil {
			return diff, err
		}
		blip.Debug("loaded %d monitors from config file", len(ml.cfg.Monitors))
	}

	// Second, monitors from the monitor files
	newConfigs, err = ml.loadFiles(ctx)
	if err != nil {
		return diff, err
	}
	if err := ml.save(newConfigs, validConfigs); err != nil {
		return diff, err
	}

	// Third, monitors from the AWS RDS API
	if len(ml.cfg.MonitorLoader.AWS.Regions) > 0 {
		newConfigs, err = ml.rdsLoader.Load(ctx, ml.cfg)
		if err != nil {
			if !ml.cfg.MonitorLoader.AWS.Automatic() {
				return diff, err
			}
			blip.Debug("failed auto-AWS loading, ignoring: %s", err)
		}
		if err := ml.save(newConfigs, validConfigs); err != nil {
			return diff, err
		}
	}

	// Last, local monitors auto-detected
	if len(validConfigs) == 0 && !ml.cfg.MonitorLoader.Local.DisableAuto {
		newConfigs, err = ml.loadLocal(ctx)
		if err != nil {
			return diff, err
		}
		if err := ml.save(newConfigs, validConfigs); err != nil {
			return diff, err
		}
	}

	// ----------------------------------------------------------------------
	// Make monitors from valid configs

MAKE_MONITORS:
	// Monitors that have been removed
	for monitorId, loaded := range ml.repo {
		if _, ok := validConfigs[monitorId]; !ok {
			diff.removed = append(diff.removed, loaded.monitor)
		}
	}

	for monitorId, cfg := range validConfigs {
		// New monitor? Yes if it doesn't already exist.
		existingMonitor, ok := ml.repo[monitorId]
		if !ok {
			newMonitor, err := ml.makeMonitor(cfg)
			if err != nil {
				return diff, err
			}
			diff.added = append(diff.added, newMonitor)
			continue
		}

		// Existing monitor, but has it changed?
		// To detect, we hash the entire config and compare the SHAs.
		// Consequently, changing a single character anywhere in the
		// config is a different (new) monitor. It's a dumb but safe
		// approach because a "smart" approach would need a lot of
		// logic to detect what changed and what to do about it.
		newHash := sha256.Sum256([]byte(fmt.Sprintf("%v", cfg)))
		oldHash := sha256.Sum256([]byte(fmt.Sprintf("%v", existingMonitor.monitor.Config())))
		if newHash == oldHash {
			continue // no change
		}
		diff.changed = append(diff.changed, existingMonitor.monitor)
		newMonitor, err := ml.makeMonitor(cfg)
		if err != nil {
			return diff, err
		}
		diff.added = append(diff.added, newMonitor)
	}

	return diff, nil
}

// save saves newConfigs to validConfigs if all new configs are valid, else it
// return an error. This function is only called in load (not Load) to initialize,
// validate, and merge (save) new monitor configs from the various sources: files,
// table, AWS, and locally auto-detectedd (or however config.monitor-loader is set).
func (ml *Loader) save(newConfigs []blip.ConfigMonitor, validConfigs map[string]blip.ConfigMonitor) error {
	for _, newcfg := range newConfigs {
		// Initialize new configure in this order:
		newcfg.ApplyDefaults(ml.cfg)              // 1. apply defaults to monitor values
		newcfg.InterpolateEnvVars()               // 2. replace ${ENV_VAR} in monitor values
		newcfg.InterpolateMonitor()               // 3. replace %{monitor.X} in monitor values
		newcfg.MonitorId = blip.MonitorId(newcfg) // 4. set monitor ID if not explicitly set

		// Validate the monitor config after it has been fully initialized
		if err := newcfg.Validate(); err != nil {
			return err
		}

		// Save valid monitor config. The does NOT create or run the monitor:
		// the former is done at the end of load (not Load), and the latter is
		// done in StartMonitors.
		blip.Debug("loaded monitor: %#v", newcfg.Redacted())
		validConfigs[newcfg.MonitorId] = newcfg
	}
	return nil
}

// makeMonitor makes a new Monitor. Normally, there'd be a factory for this,
// but Monitor are concrete, not abstract, so there's only one way to make them.
// Testing mocks the abstract parts of a Monitor, like LevelCollector and PlanChanger.
func (ml *Loader) makeMonitor(cfg blip.ConfigMonitor) (*Monitor, error) {
	// Make sinks for this monitor. Each monitor has its own sinks.
	sinks := []blip.Sink{}
	for sinkName, opts := range cfg.Sinks {
		sink, err := sink.Make(blip.SinkFactoryArgs{
			SinkName:  sinkName,
			MonitorId: cfg.MonitorId,
			Options:   opts,
			Tags:      cfg.Tags,
		})
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, sink)
		blip.Debug("%s sends to %s", cfg.MonitorId, sinkName)
	}

	// If no sinks, default to printing metrics to stdout
	if len(sinks) == 0 {
		blip.Debug("using log sink")
		sink, _ := sink.Make(blip.SinkFactoryArgs{SinkName: sink.Default, MonitorId: cfg.MonitorId})
		sinks = append(sinks, sink)
	}

	// Configure the HA Manager for the monitor
	var ham ha.Manager
	ham, err := ha.Make(cfg)
	if err != nil {
		return nil, err
	}

	mon := NewMonitor(MonitorArgs{
		Config:          cfg,
		DbMaker:         ml.factory.DbConn,
		PlanLoader:      ml.planLoader,
		Sinks:           sinks,
		HA:              ham,
		TransformMetric: ml.plugin.TransformMetrics,
	})
	return mon, nil
}

// loadFiles loads monitors from config.monitor-loader.files, if any. It only
// loads the files; it doesn't validate--that's done in save().
func (ml *Loader) loadFiles(ctx context.Context) ([]blip.ConfigMonitor, error) {
	if len(ml.cfg.MonitorLoader.Files) == 0 {
		return nil, nil
	}
	status.Blip("monitor-loader", "loading from files")

	mons := []blip.ConfigMonitor{}
	for _, file := range ml.cfg.MonitorLoader.Files {
		bytes, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var cfg blip.ConfigMonitor
		if err := yaml.Unmarshal(bytes, &cfg); err != nil {
			return nil, fmt.Errorf("%s: invalid YAML: %s", file, err)
		}
		mons = append(mons, cfg)
		blip.Debug("loaded %s", file)
	}
	return mons, nil
}

// loadLocal auto-detects local MySQL instances.
func (ml *Loader) loadLocal(ctx context.Context) ([]blip.ConfigMonitor, error) {
	status.Blip("monitor-loader", "auto-detect local")

	// Auto-detect using default MySQL username (config.mysql.username),
	// which is probably "blip". Also try "root" if not explicitly disabled.
	users := []string{ml.cfg.MySQL.Username}
	if !ml.cfg.MonitorLoader.Local.DisableAutoRoot {
		users = append(users, "root")
	}

	sockets := dbconn.Sockets()

	// For every user, try every socket, then 127.0.0.1.
USERS:
	for _, user := range users {

		cfg := blip.DefaultConfigMonitor()
		cfg.ApplyDefaults(ml.cfg)
		cfg.InterpolateEnvVars()
		moncfg := cfg
		moncfg.MonitorId = "localhost"
		moncfg.Username = user

	SOCKETS:
		for _, socket := range sockets {
			moncfg.Socket = socket
			cfg.InterpolateMonitor()

			if err := ml.testLocal(ctx, moncfg); err != nil {
				// Failed to connect
				blip.Debug("auto-detect socket %s user %s: fail: %s",
					moncfg.Socket, moncfg.Username, err)
				continue SOCKETS
			}

			// Connected via socket
			return []blip.ConfigMonitor{moncfg}, nil
		}

		// -------------------------------------------------------------------
		// TCP
		moncfg.Socket = ""
		moncfg.Hostname = "127.0.0.1:3306"
		cfg.InterpolateMonitor()

		if err := ml.testLocal(ctx, moncfg); err != nil {
			blip.Debug("local auto-detect tcp %s user %s: fail: %s",
				moncfg.Hostname, moncfg.Username, err)
			continue USERS
		}

		return []blip.ConfigMonitor{moncfg}, nil
	}

	return nil, nil
}

func (ml *Loader) testLocal(bg context.Context, moncfg blip.ConfigMonitor) error {
	db, _, err := ml.factory.DbConn.Make(moncfg)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	defer cancel()
	return db.PingContext(ctx)
}

// Monitor returns one monitor by ID.  It's used by the API to get single
// monitor status.
func (ml *Loader) Monitor(monitorId string) *Monitor {
	ml.Lock()
	defer ml.Unlock()
	m, ok := ml.repo[monitorId]
	if !ok {
		return nil
	}
	return m.monitor
}

// Monitors returns a list of all currently loaded monitors.
func (ml *Loader) Monitors() []*Monitor {
	ml.Lock()
	defer ml.Unlock()
	monitors := make([]*Monitor, len(ml.repo))
	i := 0
	for _, loaded := range ml.repo {
		monitors[i] = loaded.monitor
		i++
	}
	return monitors
}

// Count returns the number of loaded monitors. It's used by the API for status.
func (ml *Loader) Count() uint {
	ml.Lock()
	defer ml.Unlock()
	return uint(len(ml.repo))
}

// Start starts a monitor if it's not already running. If a monitor with the
// same ID is still stopping, the start is pending: the monitor starts when
// that stop finishes. Start returns nil if the monitor is already running,
// if its start is pending, or if Plugins.StartMonitor declines to start it.
// Use TryStart to know which.
//
// If lock is true, Start locks the Loader mutex. If lock is false, the caller
// must hold the Loader mutex. The lock parameter is deprecated: Blip calls
// Start only with lock true. It will be removed in v3, and Start will return
// (StartResult, error) like TryStart.
func (ml *Loader) Start(monitorId string, lock bool) error {
	if lock {
		ml.Lock()
		defer ml.Unlock()
	}
	_, err := ml.tryStart(monitorId)
	return err
}

// TryStart starts a monitor like Start (with lock true) and returns whether
// the monitor started, is pending start, was already running, or was declined
// by Plugins.StartMonitor. It returns START_ERROR and an error if the monitor
// didn't start because of an error, like ErrMonitorNotLoaded if the monitor
// isn't loaded.
//
// TryStart is an interim v2 API: in v3, it will be folded into Start, which
// will return (StartResult, error), and the lock parameter will be removed.
func (ml *Loader) TryStart(monitorId string) (StartResult, error) {
	ml.Lock()
	defer ml.Unlock()
	return ml.tryStart(monitorId)
}

func (ml *Loader) tryStart(monitorId string) (StartResult, error) {
	/* -- CALLER MUST LOCK Loader -- */
	m, ok := ml.repo[monitorId]
	if !ok {
		return START_ERROR, ErrMonitorNotLoaded
	}
	if m.startPending {
		return START_PENDING, nil
	}
	if m.started {
		return START_RUNNING, nil
	}
	return ml.start(m)
}

// Stop stops a monitor but does not unload it. It can be started again
// by calling Start.
//
// Stop waits for the monitor to stop, but no longer than the stop timeout
// (config.monitor-loader.stop-timeout). It returns ErrStopTimeout if the
// monitor is still stopping; it keeps stopping in the background. If an
// earlier monitor with the same ID is still stopping because it was unloaded
// or replaced (by Load or Unload), Stop waits for that stop the same way.
// Stop also cancels a pending start of the monitor.
//
// If lock is true, Stop locks the Loader mutex to begin the stop, then waits
// without the mutex. If lock is false, the caller must hold the Loader mutex,
// and Stop waits while the caller holds it. The lock parameter is deprecated:
// Blip calls Stop only with lock true. It will be removed in v3.
func (ml *Loader) Stop(monitorId string, lock bool) error {
	if lock {
		ml.Lock()
	}
	var sm *stoppingMonitor
	if m, ok := ml.repo[monitorId]; ok {
		m.startPending = false
		sm = ml.stop(m)
	}
	if sm == nil {
		sm = ml.stopping[monitorId] // unloaded or replaced monitor; nil if none
	}
	if lock {
		ml.Unlock()
	}
	return ml.waitStopped(context.Background(), []*stoppingMonitor{sm}, !lock)
}

// Unload stops and removes a monitor. It waits for the monitor to stop, or
// for an earlier monitor with the same ID that is still stopping, and takes
// the lock argument, which is deprecated, the same as Stop.
func (ml *Loader) Unload(monitorId string, lock bool) error {
	if lock {
		ml.Lock()
	}
	sm := ml.unload(monitorId)
	if sm == nil {
		sm = ml.stopping[monitorId] // unloaded or replaced monitor; nil if none
	}
	if lock {
		ml.Unlock()
	}
	return ml.waitStopped(context.Background(), []*stoppingMonitor{sm}, !lock)
}

// unload removes a monitor from the repo and begins stopping it, if needed.
// It returns the stop, or nil if the monitor isn't stopping. The caller must
// hold the Loader mutex.
func (ml *Loader) unload(monitorId string) *stoppingMonitor {
	m, ok := ml.repo[monitorId]
	if !ok {
		return nil
	}
	delete(ml.repo, monitorId)
	sm := ml.stop(m)
	if _, ok := ml.stopping[monitorId]; !ok {
		// Not running and no earlier monitor with this ID is stopping, so
		// nothing will report status for this ID again
		status.RemoveMonitor(monitorId)
	}
	return sm
}

// stop begins stopping a monitor in the background, if it's running. It returns
// the stop, which is nil if the monitor isn't running or stopping. The caller
// must hold the Loader mutex. Use waitStopped, without the mutex, to wait for it.
func (ml *Loader) stop(m *loadedMonitor) *stoppingMonitor {
	monitorId := m.monitor.MonitorId()
	if sm, ok := ml.stopping[monitorId]; ok {
		if sm.monitor == m.monitor {
			return sm // already stopping
		}
		return nil // m is waiting for an earlier monitor to stop, so m isn't running
	}
	if !m.started {
		return nil // not running; DO NOT call Monitor.Stop on a monitor that never started
	}
	m.started = false

	sm := &stoppingMonitor{
		monitor:  m.monitor,
		begin:    time.Now(),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	ml.stopping[monitorId] = sm
	go ml.runStop(monitorId, sm)
	return sm
}

// runStop stops a monitor in the background. It reports the monitor if it
// hasn't stopped within the stop timeout, then again every stopReportFactor x
// stop timeout until it stops. Then it sends the stopped event and calls
// stopped. Because this one goroutine sends every stop event, the stopped
// event always comes after the stop timeout events.
func (ml *Loader) runStop(monitorId string, sm *stoppingMonitor) {
	var wasRunning bool // read only after done is closed
	go func() {
		wasRunning = sm.monitor.halt()
		close(sm.done)
	}()

	stuck := false
	wait := ml.stopTimeout
	timer := time.NewTimer(wait)
	defer timer.Stop()
WAIT:
	for {
		select {
		case <-sm.done:
			break WAIT
		case <-timer.C:
		}
		if ml.stopTimedOut(monitorId, sm) {
			stuck = true
		}
		wait = stopReportFactor * ml.stopTimeout
		timer.Reset(wait)
	}

	// Event receivers run synchronously, so don't send while holding the mutex
	if wasRunning {
		d := time.Since(sm.begin)
		blip.Debug("%s stopped in %s", monitorId, d)
		if stuck {
			sm.monitor.event.Sendf(event.MONITOR_STOPPED, "stopped in %s, after the stop timeout (%s)", d, ml.stopTimeout)
		} else {
			sm.monitor.event.Sendf(event.MONITOR_STOPPED, "stopped in %s", d)
		}
	}
	ml.stopped(monitorId, sm, stuck)
}

// stopTimedOut reports a monitor that hasn't stopped, with how long it has been
// stopping, and returns true if it reported it. It returns false if the
// monitor has stopped, which it checks with the Loader mutex held because it
// can wait for the mutex. It's called only by runStop.
func (ml *Loader) stopTimedOut(monitorId string, sm *stoppingMonitor) bool {
	ml.Lock()
	select {
	case <-sm.done:
		ml.Unlock()
		return false // stopped
	default:
	}
	d := time.Since(sm.begin).Round(time.Millisecond)
	status.Monitor(monitorId, "error:stop", "not stopped after %s; stop began at %s", d, blip.FormatTime(sm.begin))
	msg := fmt.Sprintf("not stopped after %s; still stopping in the background", d)
	if m, ok := ml.repo[monitorId]; ok && m.startPending {
		msg += "; a monitor with the same ID is waiting for it to stop"
	}
	ml.Unlock()

	blip.Debug("%s: %s", monitorId, msg)
	sm.monitor.event.Error(event.MONITOR_STOP_TIMEOUT, msg)
	return true
}

// stopped is called by runStop when a monitor has stopped, after its events are
// sent. It removes the stop, and the error:stop status if the stop timed out
// (stuck), and starts the monitor with the same ID if it's waiting to start
// (startPending).
func (ml *Loader) stopped(monitorId string, sm *stoppingMonitor, stuck bool) {
	ml.Lock()
	defer ml.Unlock()
	defer close(sm.finished)
	if ml.stopping[monitorId] != sm {
		return // shouldn't happen
	}
	delete(ml.stopping, monitorId)
	if stuck {
		status.RemoveComponent(monitorId, "error:stop")
	}

	m, ok := ml.repo[monitorId]
	if !ok || m.monitor != sm.monitor {
		// Unloaded, or replaced by a new monitor with the same ID that hasn't
		// started yet. Remove the old monitor's status, like Unload did before
		// stopping in the background.
		status.RemoveMonitor(monitorId)
	}
	if ok && m.startPending {
		if _, err := ml.start(m); err != nil {
			blip.Debug("%s: start after stop: %s", monitorId, err) // shouldn't happen
		}
	}
}

// waitStopped waits for stops to finish, but no longer than the stop timeout
// or ctx. It returns ErrStopTimeout if any are still stopping. Nil stops are
// ignored.
//
// If mutexHeld is false, waitStopped waits until stopped has run for each stop,
// so the stopped monitor's status is removed (if it was unloaded) and a pending
// start of the same monitor ID has started. If mutexHeld is true, the caller
// holds the Loader mutex, and waitStopped waits only until the monitor has
// stopped (sm.done), which is closed before runStop takes the mutex (in
// stopTimedOut or stopped).
func (ml *Loader) waitStopped(ctx context.Context, stops []*stoppingMonitor, mutexHeld bool) error {
	if len(stops) == 0 {
		return nil
	}
	timeout := time.NewTimer(ml.stopTimeout)
	defer timeout.Stop()
	for _, sm := range stops {
		if sm == nil {
			continue
		}
		done := sm.finished
		if mutexHeld {
			done = sm.done
		}
		select {
		case <-done:
		case <-timeout.C:
			return ErrStopTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Print prints all loaded monitors in blip.ConfigMonitor YAML format.
// It's used for --print-monitors.
func (ml *Loader) Print() string {
	ml.Lock()
	defer ml.Unlock()
	m := make([]blip.ConfigMonitor, len(ml.repo))
	i := 0
	for monitorId := range ml.repo {
		m[i] = ml.repo[monitorId].monitor.Config()
		i++
	}
	p := printMonitors{Monitors: m}
	bytes, err := yaml.Marshal(p)
	if err != nil {
		return fmt.Sprintf("# yaml.Marshal error: %s", err) // shouldn't happen
	}
	return string(bytes)
}

// printMonitors is used by Print to output monitors in the correct YAML format.
type printMonitors struct {
	Monitors []blip.ConfigMonitor `yaml:"monitors"`
}
