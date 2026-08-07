// Copyright 2024 Block, Inc.

package server

import (
	"fmt"

	"github.com/alexflint/go-arg"
	"gopkg.in/yaml.v2"

	"github.com/cashapp/blip/v2"
)

// Options represents typical command line options: --addr, --config, etc.
type Options struct {
	Config        string `arg:"env:BLIP_CONFIG"`
	Debug         bool   `arg:"env:BLIP_DEBUG"`
	Help          bool
	Log           bool `arg:"env:BLIP_LOG"`
	PrintConfig   bool `arg:"--print-config"`
	PrintDomains  bool `arg:"--print-domains"`
	PrintMonitors bool `arg:"--print-monitors"`
	PrintPlans    bool `arg:"--print-plans"`
	Run           bool `arg:"env:BLIP_RUN" default:"true"`
	Version       bool `arg:"-v"`
}

// CommandLine represents options (--addr, etc.) and args: entity type, return
// labels, and query predicates. The caller is expected to copy and use the embedded
// structs separately, like:
//
//	var o config.Options = cmdLine.Options
//	for i, arg := range cmdline.Args {
type CommandLine struct {
	Options
	Args []string `arg:"positional"`
}

// ParseCommandLine parses the command line and env vars. Command line options
// override env vars. Default options are used unless overridden by env vars or
// command line options. Defaults are usually parsed from config files.
func ParseCommandLine(args []string) (CommandLine, error) {
	var c CommandLine
	p, err := arg.NewParser(arg.Config{Program: "blip"}, &c)
	if err != nil {
		return c, err
	}
	if err := p.Parse(args); err != nil {
		switch err {
		case arg.ErrHelp:
			c.Help = true
		case arg.ErrVersion:
			c.Version = true
		default:
			return c, fmt.Errorf("Error parsing command line: %s\n", err)
		}
	}
	return c, nil
}

func printHelp() {
	fmt.Printf("Usage:\n"+
		"  blip [options]\n\n"+
		"Options:\n"+
		"  --config         Config file (default: %s)\n"+
		"  --debug          Print debug to stderr\n"+
		"  --help           Print help and exit\n"+
		"  --log            Log info events to STDOUT\n"+
		"  --print-config   Print config on boot\n"+
		"  --print-domains  Print metric domains\n"+
		"  --print-monitors Print monitors on boot\n"+
		"  --run            Run monitors (if false, boot then exit)\n"+
		"  --version        Print version and exit\n"+
		"\n"+
		"blip %s\n",
		blip.DEFAULT_CONFIG_FILE, blip.VERSION,
	)
}

func printYAML(v interface{}) {
	bytes, err := yaml.Marshal(v)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(bytes))
}
