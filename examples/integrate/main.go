package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/heartbeat"
	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/server"
)

func main() {
	// Start with Blip defaults
	env, plugins, factories := server.Defaults()

	// ----------------------------------------------------------------------
	// Integration

	// Register a custom metrics collector for domain "foo"
	metrics.Register("foo", FooFactory{})

	// Set a custom plugin
	plugins.LoadConfig = func(cfg blip.Config) (blip.Config, error) {
		// Custom config loading logic
		blip.Debug("plugins.LoadConfig called")
		return cfg, nil
	}

	// Set a custom factory
	factories.DbConn = dbFactory{}

	// Set a variable to a custom value
	heartbeat.NoHeartbeatWait = 1 * time.Minute

	// ----------------------------------------------------------------------
	// Create, boot, and run the customized Blip server
	s := server.Server{}
	if err := s.Boot(env, plugins, factories); err != nil {
		log.Fatalf("server.Boot failed: %s", err)
	}
	if err := s.Run(server.ControlChans()); err != nil { // blocking
		log.Fatalf("server.Run failed: %s", err)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Custom metrics collector

// FooMetrics collects metrics for the "foo" domain
type FooMetrics struct {
	monitorId string
	db        *sql.DB
}

var _ blip.Collector = &FooMetrics{}

func NewFooMetrics(monitorId string, db *sql.DB) *FooMetrics {
	return &FooMetrics{
		monitorId: monitorId,
		db:        db,
	}
}

func (c *FooMetrics) Domain() string {
	return "foo"
}

func (c *FooMetrics) Help() blip.CollectorHelp {
	return blip.CollectorHelp{}
}

func (c *FooMetrics) Prepare(ctx context.Context, plan blip.Plan) (func(), error) {
	return nil, nil
}

func (c *FooMetrics) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	return nil, nil
}

// FooFactory makes FooMetrics collectors for the "foo" domain
type FooFactory struct{}

func (f FooFactory) Make(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
	return NewFooMetrics(args.MonitorId, args.DB), nil
}

// //////////////////////////////////////////////////////////////////////////
// Custom *sql.DB factory

// dbFactory makes custom *sql.DB connections for each monitor
type dbFactory struct{}

var _ blip.DbFactory = dbFactory{}

func (f dbFactory) Make(blip.ConfigMonitor) (*sql.DB, string, error) {
	blip.Debug("dbFactory called")
	db, err := sql.Open("mysql", "user@tcp(127.0.0.1)/")
	if err != nil {
		return nil, "", err
	}
	return db, "log-safe-dsn", nil
}
