// Copyright 2024 Block, Inc.

// Package server provides the core runtime for Blip.
package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/sink"
	"github.com/cashapp/blip/v2/status"
)

// ControlChans is a convenience function to return arguments for Run.
func ControlChans() (stopChan, doneChan chan struct{}) {
	return make(chan struct{}), make(chan struct{})
}

// Defaults returns the default environment, plugins, and factories. It is used
// in main.go as the args to Server.Boot. Third-party integration will likely
// _not_ call this function and, instead, provide its own environment, plugins,
// or factories when calling Server.Boot.
func Defaults() (blip.Env, blip.Plugins, blip.Factories) {
	factories := blip.Factories{
		AWSConfig: &aws.ConfigFactory{},
		// DbConn made after loading config
		// HTTPClient made after loading config
	}
	env := blip.Env{
		Args: os.Args,
		Env:  os.Environ(),
	}
	return env, blip.Plugins{}, factories
}

type httpClientFactory struct {
	cfg blip.ConfigHTTP
}

func (f httpClientFactory) MakeForSink(sinkName, monitorId string, opts, tags map[string]string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.ResponseHeaderTimeout = 5 * time.Second
	transport.ExpectContinueTimeout = 1 * time.Second
	transport.IdleConnTimeout = 30 * time.Second
	transport.MaxIdleConnsPerHost = 2
	if f.cfg.Proxy != "" {
		proxyFunc := func(req *http.Request) (url *url.URL, err error) {
			return url.Parse(f.cfg.Proxy)
		}
		transport.Proxy = proxyFunc
		blip.Debug("%s sink %s http proxy via %s", monitorId, sinkName, f.cfg.Redacted().Proxy)
	}
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}, nil
}

// --------------------------------------------------------------------------

// Server is the core runtime for one instance of Blip. It's responsible for
// starting and running everything: Boot and Run, respectively. As long as
// the server is running, Blip is running.
//
// See bin/blip/main.go for the simplest use case.
type Server struct {
	cfg           blip.Config
	cmdline       CommandLine
	planLoader    *plan.Loader
	monitorLoader *monitor.Loader
	api           *API
}

// Boot boots Blip. That means it loads, validates, and creates everything,
// but it doesn't start (run) anything--that happens when Run is called.
// If Boot returns nil, then Blip is ready to run; else, any error is fatal:
// sometimes is too wrong for Blip to run (for example, an invalid config).
//
// Boot must be called once before Run.
func (s *Server) Boot(env blip.Env, plugins blip.Plugins, factories blip.Factories) error {
	// ----------------------------------------------------------------------
	// Parse commad line options
	// ----------------------------------------------------------------------

	var err error
	s.cmdline, err = ParseCommandLine(env.Args)
	if err != nil {
		return err
	}

	// Set global debug var first because all code calls blip.Debug
	blip.Debugging = s.cmdline.Options.Debug
	blip.Debug("blip %s %+v", blip.VERSION, s.cmdline)

	// Return early (don't boot/run) --help, --verison, and --print-domains
	if s.cmdline.Options.Help {
		printHelp()
		os.Exit(0)
	}
	if s.cmdline.Options.Version {
		fmt.Println("blip", blip.VERSION)
		os.Exit(0)
	}
	if s.cmdline.Options.PrintDomains {
		fmt.Fprintf(os.Stdout, "%s", metrics.PrintDomains())
		os.Exit(0)
	}

	// ----------------------------------------------------------------------
	// Boot sequence
	// ----------------------------------------------------------------------

	startTs := time.Now()
	event.SetReceiver(event.Log{All: s.cmdline.Options.Log})
	event.Sendf(event.BOOT_START, "blip %s", blip.VERSION) // very first event

	status.Blip("started", "%s", blip.FormatTime(startTs))
	status.Blip("version", "%s", blip.VERSION)
	status.Blip(status.SERVER, "%s", "booting")

	// ----------------------------------------------------------------------
	// Load config
	event.Send(event.BOOT_CONFIG_LOADING)
	status.Blip(status.SERVER, "boot: loading config")

	// LoadConfig plugins takes priority if defined. Else, load --config file,
	// which defaults to blip.yaml.
	if plugins.LoadConfig != nil {
		blip.Debug("call plugins.LoadConfig")
		s.cfg, err = plugins.LoadConfig(blip.DefaultConfig())
		// Do not apply defaults; plugin is responsible for that in case
		// it wants full control of the config (which isn't advised but allowed).
	} else {
		// If --config specified, then file is required to exist.
		// If not specified, then use default if it exist (not required).
		// If default file doesn't exist, then Blip will run with a
		// full default config (i.e. the zero config).
		required := true
		if s.cmdline.Options.Config == "" {
			s.cmdline.Options.Config = blip.DEFAULT_CONFIG_FILE
			required = false
		}
		s.cfg, err = blip.LoadConfig(s.cmdline.Options.Config, blip.DefaultConfig(), required)

		// Apply config file on top of defaults, so if a value is set in the config
		// file, it overrides the default value (if any)
		s.cfg.ApplyDefaults(blip.DefaultConfig())
	}
	if err != nil {
		event.Sendf(event.BOOT_ERROR, "%s", err.Error()) //nolint:vet
		return err
	}
	s.cfg.InterpolateEnvVars()
	blip.Debug("config: %#v", s.cfg.Redacted())
	if err := s.cfg.Validate(); err != nil {
		event.Errorf(event.BOOT_CONFIG_INVALID, "%s", err.Error())
		return err
	}
	event.Send(event.BOOT_CONFIG_LOADED)

	if s.cmdline.Options.PrintConfig {
		printYAML(s.cfg)
	}

	// ----------------------------------------------------------------------
	// Finished and register Blip global factories

	// If HTTP factory not provided, then use default with config.http, which
	// is why its creation is delayed until now
	if factories.HTTPClient == nil {
		factories.HTTPClient = httpClientFactory{cfg: s.cfg.HTTP}
	}

	if factories.DbConn == nil {
		factories.DbConn = dbconn.NewConnFactory(
			factories.AWSConfig,
			plugins.ModifyDB,
			dbconn.WithPasswordSecretParser(plugins.ParsePasswordSecret),
		)
	}

	sink.InitFactory(factories)
	metrics.InitFactory(factories)

	// ----------------------------------------------------------------------
	// Load level plans
	status.Blip(status.SERVER, "boot: loading plans")

	// Get the built-in level plan loader singleton. It's used in two places:
	// here for initial plan loading, and by the monitor.LevelCollector to fetch
	// whatever plan it's told to collect.
	s.planLoader = plan.NewLoader(plugins.LoadPlans)

	if err := s.planLoader.LoadShared(s.cfg.Plans, factories.DbConn); err != nil {
		event.Sendf(event.BOOT_ERROR, "%s", err.Error())
		return err
	}

	if s.cmdline.Options.PrintPlans {
		s.planLoader.Print()
	}

	// ----------------------------------------------------------------------
	// Load monitors
	status.Blip(status.SERVER, "boot: load monitors")

	// Create, but don't start, database monitors. They're started later in Run.
	s.monitorLoader = monitor.NewLoader(monitor.LoaderArgs{
		Config:     s.cfg,
		Factories:  factories,
		Plugins:    plugins,
		PlanLoader: s.planLoader,
		RDSLoader:  aws.RDSLoader{ClientFactory: aws.NewRDSClientFactory(factories.AWSConfig)},
	})
	if err := s.monitorLoader.Load(context.Background()); err != nil {
		event.Sendf(event.BOOT_ERROR, "%s", err.Error())
		return err
	}

	if s.cmdline.Options.PrintMonitors {
		fmt.Println(s.monitorLoader.Print())
	}

	// ----------------------------------------------------------------------
	// API
	if !s.cfg.API.Disable {
		s.api = NewAPI(s.cfg, s.monitorLoader)
	} else {
		blip.Debug("API disabled")
	}

	event.Sendf(event.BOOT_SUCCESS, "booted in %s, loaded %d monitors", time.Now().Sub(startTs), s.monitorLoader.Count())
	return nil // ok to call Run
}

func (s *Server) Run(stopChan, doneChan chan struct{}) error {
	defer close(doneChan)

	// Return if --run=false (boot blip/server but don't run)
	if !s.cmdline.Options.Run {
		return nil
	}
	event.Send(event.SERVER_RUN)

	stopReason := "unknown"
	defer func() {
		event.Errorf(event.SERVER_STOPPED, "%s", stopReason)
	}()

	// Start all monitors. Then if config.monitor-load.freq is specified, start
	// periodical monitor reloading.
	status.Blip(status.SERVER, "starting monitors")
	s.monitorLoader.StartMonitors()

	// Run API, restart on panic
	if !s.cfg.API.Disable {
		go s.api.Run()
	}

	// Run until caller closes stopChan or blip process catches a signal
	status.Blip(status.SERVER, "running since %s", blip.FormatTime(time.Now()))
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)
	defer signal.Stop(signalChan)
	for {
		select {
		case <-stopChan:
			stopReason = "server stopped"
			return nil
		case s := <-signalChan:
			switch s {
			case os.Interrupt, syscall.SIGTERM:
				stopReason = "caught signal"
				return nil
			case syscall.SIGUSR1:
				blip.Debugging = !blip.Debugging
				fmt.Fprintf(os.Stderr, "SIGUSR1 has set blip.Debugging to %t\n", blip.Debugging)
			}
		}
	}
}
