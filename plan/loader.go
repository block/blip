// Package plan provides the Loader singleton that loads metric collection plans.
package plan

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/event"
	"github.com/cashapp/blip/metrics"
	default_plan "github.com/cashapp/blip/plan/default"
	"github.com/cashapp/blip/sqlutil"
)

// Meta is a blip.Plan plus metadata.
type Meta struct {
	Name   string
	Source string
	Shared bool
	YAML   string // plan converted to YAML
	plan   blip.Plan
}

// Loader is a singleton service and repo for loading plans.
type Loader struct {
	plugin       func(blip.ConfigPlans) ([]blip.Plan, error)
	sharedPlans  []Meta            // keyed on Plan.Name
	monitorPlans map[string][]Meta // keyed on monitorId, Plan.Name
	*sync.RWMutex
}

func NewLoader(plugin func(blip.ConfigPlans) ([]blip.Plan, error)) *Loader {
	return &Loader{
		plugin:       plugin,
		sharedPlans:  []Meta{},
		monitorPlans: map[string][]Meta{},
		RWMutex:      &sync.RWMutex{},
	}
}

// LoadShared loads all top-level (shared) plans: config.Plans. These plans are
// called "shared" because more than one monitor can use them, which is the normal
// case. For example, the simplest configuration is specifying a single shared plan
// that almost monitors use implicitly (by not specifying config.monitors.*.Plans).
//
// This method is called by Server.Boot(). Plans from a table are deferred until
// the monitor's LCO calls Plan() because the monitor might not be online when Blip
// starts.
func (pl *Loader) LoadShared(cfg blip.ConfigPlans, dbMaker blip.DbFactory) error {
	event.Send(event.PLANS_LOAD_SHARED)

	// If LoadPlans plugin is defined, it does all the work: call and return early
	if pl.plugin != nil {
		blip.Debug("loading plans from plugin")
		plans, err := pl.plugin(cfg)
		if err != nil {
			return err
		}
		if len(plans) == 0 {
			return fmt.Errorf("LoadPlans plugin returned zero plans, expected at least one in strict mode")
		}
		if err := ValidatePlans(plans); err != nil {
			return err
		}

		pl.Lock()
		pl.sharedPlans = make([]Meta, len(plans))
		for i, plan := range plans {
			pl.sharedPlans[i] = Meta{
				Name:   plan.Name,
				plan:   plan,
				Source: "plugin",
			}
		}
		pl.Unlock()

		return nil
	}

	sharedPlans := []Meta{}

	// Read default plans from table on pl.cfg.Plans.monitor
	if cfg.Table != "" {
		blip.Debug("loading plans from %s", cfg.Table)

		// Connect to db specified by config.Plans.monitor, which should have
		// been validated already, but double check. It reuses ConfigMonitor
		// for the DSN info, not because it's an actual db to monitor.
		if cfg.Monitor == nil {
			return fmt.Errorf("Table set but Monitor is nil")
		}

		db, _, err := dbMaker.Make(*cfg.Monitor)
		if err != nil {
			return err
		}
		defer db.Close()

		// Last arg "" = no monitorId, read all rows
		plans, err := ReadTable(cfg.Table, db, "")
		if err != nil {
			return err
		}

		if err := ValidatePlans(plans); err != nil {
			return err
		}

		// Save all plans from table by name
		for _, plan := range plans {
			sharedPlans = append(sharedPlans, Meta{
				Name:   plan.Name,
				plan:   plan,
				Source: cfg.Table,
				Shared: true,
			})
		}
	}

	// Read all plans from all files
	if len(cfg.Files) > 0 {
		blip.Debug("loading shared plans from %v", cfg.Files)
		plans, err := pl.readPlans(cfg.Files)
		if err != nil {
			blip.Debug(err.Error())
			return err
		}

		// Save all plans from table by name
		for _, pm := range plans {
			sharedPlans = append(sharedPlans, pm)
		}
	}

	// Load default plans if no shared plans are specified and default plans
	// aren't disabled. This copies the default into the shared plans repo.
	if len(sharedPlans) == 0 && !cfg.DisableDefaultPlans {
		blip.Debug("default plans enabled")
		dplanMySQL := default_plan.MySQL() // MySQL
		sharedPlans = append(sharedPlans, Meta{
			Name:   dplanMySQL.Name,
			Source: dplanMySQL.Source,
			plan:   dplanMySQL,
			Shared: true,
		})
		dplanExprter := default_plan.Exporter() // Prom mysqld_exporter
		sharedPlans = append(sharedPlans, Meta{
			Name:   dplanExprter.Name,
			Source: dplanExprter.Source,
			plan:   dplanExprter,
			Shared: true,
		})
	}

	pl.Lock()
	pl.sharedPlans = sharedPlans
	pl.Unlock()

	return nil
}

// LoadMonitor loads monitor plans: config.monitors.*.Plans.
func (pl *Loader) LoadMonitor(mon blip.ConfigMonitor, dbMaker blip.DbFactory) error {
	event.Sendf(event.PLANS_LOAD_MONITOR, "%s", mon.MonitorId)

	if mon.Plans.Table == "" && len(mon.Plans.Files) == 0 {
		blip.Debug("monitor %s uses only shared plans", mon.MonitorId)
		return nil
	}

	monitorPlans := []Meta{}

	// Monitor plans from table, but defer until monitor's LevelCollector calls Plan()
	if mon.Plans.Table != "" {
		table := mon.Plans.Table
		blip.Debug("%s: loading plans from table %s", mon.MonitorId, table)

		db, _, err := dbMaker.Make(mon)
		if err != nil {
			return err
		}
		defer db.Close()

		plans, err := ReadTable(table, db, mon.MonitorId)
		if err != nil {
			return nil
		}

		if err := ValidatePlans(plans); err != nil {
			return err
		}

		for _, plan := range plans {
			monitorPlans = append(monitorPlans, Meta{
				Name:   plan.Name,
				plan:   plan,
				Source: table,
			})
		}
	}

	// Monitor plans from files, load all
	if len(mon.Plans.Files) > 0 {
		blip.Debug("loading monitor %s plans from %s", mon.MonitorId, mon.Plans.Files)
		plans, err := pl.readPlans(mon.Plans.Files)
		if err != nil {
			return err
		}
		for _, pm := range plans {
			monitorPlans = append(monitorPlans, pm)
		}
	}

	pl.Lock()
	pl.monitorPlans[mon.MonitorId] = monitorPlans
	pl.Unlock()
	blip.Debug("loaded plans for monitor %s", mon.MonitorId)

	return nil
}

// Plan returns the plan for the given monitor.
func (pl *Loader) Plan(monitorId string, planName string, db *sql.DB) (blip.Plan, error) {
	pl.RLock()
	defer pl.RUnlock()

	// Point plans to either the monitor plans or shared plans. Monitors plans
	// take precedence if set; else fall back to shared plans. Related docs:
	// https://block.github.io/blip/plans/loading/
	//
	// DO NOT MODIFY plans, else you'll modify the underlying slice because Go
	// slices are refs.
	var plans []Meta
	if len(pl.monitorPlans[monitorId]) > 0 {
		blip.Debug("%s: using monitor plans", monitorId)
		plans = pl.monitorPlans[monitorId]
	} else {
		blip.Debug("%s: using shared plans", monitorId)
		plans = pl.sharedPlans
	}

	if len(plans) == 0 {
		return blip.Plan{}, fmt.Errorf("no plans loaded for monitor %s", monitorId)
	}

	var pm *Meta
	if planName == "" {
		pm = &plans[0]
		planName = pm.Name
		blip.Debug("%s: loading first plan: %s", monitorId, planName)
	} else {
		blip.Debug("%s: loading plan %s", monitorId, planName)
		for i := range plans {
			if plans[i].Name == planName {
				pm = &plans[i]
			}
		}
		if pm == nil {
			return blip.Plan{}, fmt.Errorf("monitor %s has no plan named %s", monitorId, planName)
		}
	}

	if pm.Shared {
		blip.Debug("%s: loading plan %s (shared)", monitorId, pm.Name)
		pm = nil
		for i := range pl.sharedPlans {
			if pl.sharedPlans[i].Name == planName {
				pm = &pl.sharedPlans[i]
			}
		}
		if pm == nil {
			return blip.Plan{}, fmt.Errorf("monitor %s uses shared plan %s but it was not loaded", monitorId, planName)
		}
	}

	blip.Debug("%s: loading plan %s from %s", monitorId, planName, pm.Source)
	// Since blip.Plan has field types that pass by reference (maps and slices), we want to the returned plan to
	// be a deep copy to ensure the caller cannot modify the original shared plan.
	return deepcopyPlan(&pm.plan)
}

func (pl *Loader) SharedPlans() []Meta {
	pl.RLock()
	defer pl.RUnlock()
	plans := make([]Meta, len(pl.sharedPlans))
	for i := range pl.sharedPlans {
		meta := pl.sharedPlans[i]
		bytes, _ := yaml.Marshal(pl.sharedPlans[i].plan.Levels)
		meta.YAML = string(bytes)
		plans[i] = meta
	}
	return plans
}

func (pl *Loader) MonitorPlans(ids ...string) map[string][]Meta {
	var allow map[string]bool
	if len(ids) > 0 {
		allow = map[string]bool{}
		for _, id := range ids {
			allow[id] = true
		}
	}
	plans := map[string][]Meta{}
	for monitorId, monitorPlans := range pl.monitorPlans {
		if len(allow) > 0 && !allow[monitorId] {
			continue
		}
		plans[monitorId] = make([]Meta, len(plans[monitorId]))
		for i := range monitorPlans {
			meta := monitorPlans[i]
			bytes, _ := yaml.Marshal(monitorPlans[i].plan.Levels)
			meta.YAML = string(bytes)
			plans[monitorId][i] = meta
		}
	}

	return plans
}

func (pl *Loader) Print() {
	pl.RLock()
	defer pl.RUnlock()
	var bytes []byte

	for i := range pl.sharedPlans {
		bytes, _ = yaml.Marshal(pl.sharedPlans[i].plan.Levels)
		fmt.Printf("---\n# %s\n%s\n\n", pl.sharedPlans[i].plan.Name, string(bytes))
	}
	/*
		if len(pl.monitorPlans) > 0 {
			bytes, _ = yaml.Marshal(pl.monitorPlans)
			fmt.Printf("---\n%s\n\n", string(bytes))
		} else {
			fmt.Printf("---\n# No monitor plans\n\n")
		}
	*/
}

func (pl *Loader) readPlans(filePaths []string) ([]Meta, error) {
	meta := []Meta{}       // return value
	plans := []blip.Plan{} // ValidatePlans()

	for _, filePattern := range filePaths {
		files, err := filepath.Glob(filePattern)
		if err != nil {
			return nil, err
		}
		blip.Debug("files in %s: %v", filePattern, files)

	FILES:
		for _, file := range files {
			if pl.fileLoaded(file) {
				blip.Debug("already read %s", file)
				pm := Meta{
					Name:   file,
					Shared: true,
				}
				meta = append(meta, pm)
				continue FILES
			}

			fileabs, err := filepath.Abs(file)
			if err != nil {
				blip.Debug("%s does not exist (abs), skipping")
				return nil, err
			}

			if _, err := os.Stat(file); err != nil {
				return nil, fmt.Errorf("config file %s (%s) does not exist", file, fileabs)
			}

			plan, err := ReadFile(file)
			if err != nil {
				blip.Debug("cannot read %s (%s), skipping: %s", file, fileabs, err)
				continue FILES
			}

			pm := Meta{
				Name:   file,
				plan:   plan,
				Source: fileabs,
			}
			meta = append(meta, pm)
			plans = append(plans, plan) // validate later
			blip.Debug("loaded file %s (%s) as plan %s", file, fileabs, plan.Name)
		}
	}

	if err := ValidatePlans(plans); err != nil {
		return nil, err
	}

	return meta, nil
}

func (pl *Loader) fileLoaded(file string) bool {
	for i := range pl.sharedPlans {
		if pl.sharedPlans[i].Name == file {
			return true
		}
	}
	return false
}

// --------------------------------------------------------------------------

type planFile map[string]*blip.Level

func ReadFile(file string) (blip.Plan, error) {
	bytes, err := os.ReadFile(file)
	if err != nil {
		return blip.Plan{}, err
	}

	var pf planFile
	if err := yaml.Unmarshal(bytes, &pf); err != nil {
		return blip.Plan{}, fmt.Errorf("cannot decode YAML in %s: %s", file, err)
	}

	levels := make(map[string]blip.Level, len(pf))
	for k := range pf {
		levels[k] = blip.Level{
			Name:    k, // must have, levels are collected by name
			Freq:    pf[k].Freq,
			Collect: pf[k].Collect,
		}
	}

	plan := blip.Plan{
		Name:   file,
		Levels: levels,
		Source: file,
	}
	return plan, nil
}

func ReadVariable(strVal, planName string) (blip.Plan, error) {
	var pf planFile
	if err := yaml.Unmarshal([]byte(strVal), &pf); err != nil {
		return blip.Plan{}, fmt.Errorf("cannot decode YAML: %s", err)
	}

	levels := make(map[string]blip.Level, len(pf))
	for k := range pf {
		levels[k] = blip.Level{
			Name:    k, // must have, levels are collected by name
			Freq:    pf[k].Freq,
			Collect: pf[k].Collect,
		}
	}

	plan := blip.Plan{
		Name:   planName,
		Levels: levels,
		Source: "variable",
	}
	return plan, nil
}

func ReadTable(table string, db *sql.DB, monitorId string) ([]blip.Plan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	table = sqlutil.SanitizeTable(table, blip.DEFAULT_DATABASE)
	q := fmt.Sprintf("SELECT name, plan, COALESCE(monitorId, '') FROM `%s`", table)
	if monitorId != "" {
		q += " WHERE monitorId = ? ORDER BY name ASC"
	}
	rows, err := db.QueryContext(ctx, q, monitorId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	plans := []blip.Plan{}
	for rows.Next() {
		var plan blip.Plan
		var levels string
		err := rows.Scan(&plan.Name, &levels, &plan.MonitorId)
		if err != nil {
			return nil, err
		}
		err = yaml.Unmarshal([]byte(levels), &plan.Levels)
		if err != nil {
			return nil, err
		}
		plan.Source = table
		plans = append(plans, plan)
	}

	return plans, nil
}

// ValidatePlans returns nil if all plans are valid, else it returns an error
// that lists each validation error.
func ValidatePlans(plans []blip.Plan) error {
	errMsgs := []string{}
	mcList := map[string]blip.Collector{}

	for i := range plans {

		// First level validation: Plan does its own static analysis (e.g. check freq)
		if err := plans[i].Validate(); err != nil {
			errMsgs = append(errMsgs, fmt.Sprintf("invalid plan: %s: %s", plans[i].Name, err))
			continue
		}

		// Second level validation: PlanLoader checks that domains exist, and
		// domain options vs collector help
		for levelName := range plans[i].Levels {
		DOMAINS:
			for domainName := range plans[i].Levels[levelName].Collect {

				// Make collector if needed. We're not actually running the
				// collector, so blip.CollectorFactoryArgs{} is fine (i.e.
				// don't need a *sql.DB or anything).
				mc, ok := mcList[domainName]
				if !ok {

					// Implicit domain check: if the domain in the plan causes
					// a collectory factory error, then the domain is invalid/
					// doesn't exist
					var err error
					mc, err = metrics.Make(domainName, blip.CollectorFactoryArgs{Validate: true})
					if err != nil {
						errMsgs = append(errMsgs, fmt.Sprintf("invalid plan: %s: at %s/%s: %s",
							plans[i].Name, levelName, domainName, err))
						continue DOMAINS
					}

					mcList[domainName] = mc
				}

				// Validate collector options given in plan. Help() returns
				// a blip.CollectorHelp struct which knows how to validate
				// the input options because it (the struct) contains all the
				// valid options.
				err := mc.Help().Validate(plans[i].Levels[levelName].Collect[domainName].Options)
				if err != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("invalid plan: %s: at %s/%s: %s",
						plans[i].Name, levelName, domainName, err))
				}
			}
		}
	}

	// Third level validation is each collector Prepare, called by monitor/Engine.Prepare

	if len(errMsgs) > 0 {
		return fmt.Errorf("%d plan validation errors:\n%s", len(errMsgs), strings.Join(errMsgs, "\n"))
	}

	return nil
}

func deepcopyPlan(p *blip.Plan) (blip.Plan, error) {
	// Since the operation needed here is not performance critical we can piggy back off of gob
	// encode/decode to make the deep copy and not rely on staying up-to-date with the blip.Plan's
	// internal structure.
	var (
		b   bytes.Buffer
		ret blip.Plan
	)

	if err := gob.NewEncoder(&b).Encode(p); err != nil {
		return ret, err
	}

	if err := gob.NewDecoder(&b).Decode(&ret); err != nil {
		return ret, err
	}

	return ret, nil
}
