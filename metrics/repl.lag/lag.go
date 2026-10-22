// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/heartbeat"
)

const (
	DOMAIN = "repl.lag"

	OPT_HEARTBEAT_SOURCE_ID   = "source-id"
	OPT_HEARTBEAT_SOURCE_ROLE = "source-role"
	OPT_HEARTBEAT_TABLE       = "table"
	OPT_WRITER                = "writer"
	OPT_REPL_CHECK            = "repl-check"
	OPT_REPORT_NO_HEARTBEAT   = "report-no-heartbeat"
	OPT_REPORT_NOT_A_REPLICA  = "report-not-a-replica"
	OPT_DEFAULT_CHANNEL_NAME  = "default-channel-name"
	OPT_NETWORK_LATENCY       = "network-latency"

	LAG_WRITER_BLIP = "blip"
	LAG_WRITER_PFS  = "pfs"
)

type Lag struct {
	db                          *sql.DB
	lagReader                   heartbeat.Reader
	stopReader                  func()                                          // stops lagReader; returned by Prepare as cleanup
	newReader                   func(heartbeat.BlipReaderArgs) heartbeat.Reader // tests replace it to inspect the args
	lagWriterIn                 map[string]string
	dropNoHeartbeat             map[string]bool
	dropNotAReplica             map[string]bool
	defaultChannelNameOverrides map[string]string
	replCheck                   string
	pfsLagLastQueued            map[string]string
	pfsLagLastProc              map[string]string
}

var _ blip.Collector = &Lag{}

func newBlipReader(args heartbeat.BlipReaderArgs) heartbeat.Reader {
	return heartbeat.NewBlipReader(args)
}

func NewLag(db *sql.DB) *Lag {
	return &Lag{
		db:                          db,
		newReader:                   newBlipReader,
		lagWriterIn:                 map[string]string{},
		dropNoHeartbeat:             map[string]bool{},
		dropNotAReplica:             map[string]bool{},
		defaultChannelNameOverrides: map[string]string{},
		pfsLagLastQueued:            make(map[string]string),
		pfsLagLastProc:              make(map[string]string),
	}
}

func (c *Lag) Domain() string {
	return DOMAIN
}

func (c *Lag) Help() blip.CollectorHelp {
	return blip.CollectorHelp{
		Domain:      DOMAIN,
		Description: "Replication lag",
		Options: map[string]blip.CollectorHelpOption{
			OPT_WRITER: {
				Name:    OPT_WRITER,
				Desc:    "How to collect Lag",
				Default: "auto",
				Values: map[string]string{
					"auto": "Auto-determine best lag writer",
					"blip": "Native Blip heartbeat replication lag",
					"pfs":  "Performance Schema",
					///"legacy": "Second_Behind_Slave|Replica from SHOW SHOW|REPLICA STATUS",
				},
			},
			OPT_HEARTBEAT_TABLE: {
				Name:    OPT_HEARTBEAT_TABLE,
				Desc:    "Heartbeat table",
				Default: blip.DEFAULT_HEARTBEAT_TABLE,
			},
			OPT_HEARTBEAT_SOURCE_ID: {
				Name: OPT_HEARTBEAT_SOURCE_ID,
				Desc: "Source ID as reported by heartbeat writer; mutually exclusive with " + OPT_HEARTBEAT_SOURCE_ROLE,
			},
			OPT_HEARTBEAT_SOURCE_ROLE: {
				Name: OPT_HEARTBEAT_SOURCE_ROLE,
				Desc: "Source role as reported by heartbeat writer; mutually exclusive with " + OPT_HEARTBEAT_SOURCE_ID,
			},
			OPT_REPL_CHECK: {
				Name: OPT_REPL_CHECK,
				Desc: "MySQL global variable (without @@) to check if instance is a replica",
			},
			OPT_REPORT_NO_HEARTBEAT: {
				Name:    OPT_REPORT_NO_HEARTBEAT,
				Desc:    "Report no heartbeat as -1",
				Default: "no",
				Values: map[string]string{
					"yes": "Enabled: report no heartbeat as repl.lag.current = -1",
					"no":  "Disabled: drop repl.lag.current if no heartbeat",
				},
			},
			OPT_REPORT_NOT_A_REPLICA: {
				Name:    OPT_REPORT_NOT_A_REPLICA,
				Desc:    "Report not a replica as -1",
				Default: "no",
				Values: map[string]string{
					"yes": "Enabled: report not a replica repl.lag.current = -1",
					"no":  "Disabled: drop repl.lag.current if not a replica",
				},
			},
			OPT_DEFAULT_CHANNEL_NAME: {
				Name: OPT_DEFAULT_CHANNEL_NAME,
				Desc: "Rename default replication channel name (MySQL is default an empty string)",
			},
			OPT_NETWORK_LATENCY: {
				Name:    OPT_NETWORK_LATENCY,
				Desc:    "Network latency (milliseconds)",
				Default: "50",
			},
		},
		Metrics: []blip.CollectorMetric{
			{
				Name: "current",
				Type: blip.GAUGE,
				Desc: "Current replication lag (milliseconds)",
			},
			{
				Name: "backlog",
				Type: blip.GAUGE,
				Desc: "Replication backlog (number of transactions)",
			},
			{
				Name: "worker_usage",
				Type: blip.GAUGE,
				Desc: "Replication worker usage (percentage)",
			},
		},
	}
}

// Prepare prepares one lag collector for all levels in the plan. Lag can
// (and probably will be) collected at multiple levels, but this domain can
// be configured at only one level. For example, it's not possible to collect
// lag from a Blip heartbeat and from Performance Schema. And since this
// domain collects only one metric (repl.lag.current), there's no need to
// collect different metrics at different frequencies.
//
// One level sets up the writer: the first level, by name, that sets the
// writer option to pfs or blip; else the first level that collects lag, which
// auto-detects the writer. Levels that don't set the writer, or set it to
// auto, use that writer. It's an error if one level sets pfs and another sets
// blip. The report-no-heartbeat, report-not-a-replica, and
// default-channel-name options are per level.
//
// If lag is collected from a Blip heartbeat, the returned cleanup func stops
// the heartbeat reader; else it's nil. On error, Prepare stops the reader
// and returns a nil cleanup func, as required by blip.Collector.
func (c *Lag) Prepare(ctx context.Context, plan blip.Plan) (cleanup func(), err error) {
	// The engine discards this collector if Prepare fails, so stop the
	// reader if it was started before the error.
	defer func() {
		if err != nil && c.stopReader != nil {
			c.stopReader()
		}
	}()

	// Levels that collect lag, sorted so the result doesn't depend on map order
	levels := []string{}
	for levelName, level := range plan.Levels {
		if _, ok := level.Collect[DOMAIN]; ok {
			levels = append(levels, levelName)
		}
	}
	if len(levels) == 0 {
		return nil, nil
	}
	sort.Strings(levels)

	// Find the level that sets up the writer
	configLevel := levels[0]
	writer := "auto"
	for _, levelName := range levels {
		opt := plan.Levels[levelName].Collect[DOMAIN].Options[OPT_WRITER]
		switch opt {
		case "", "auto":
			continue
		case LAG_WRITER_PFS, LAG_WRITER_BLIP:
		default:
			return nil, fmt.Errorf("invalid lag writer: %q; valid values: auto, pfs, blip", opt)
		}
		if writer == "auto" {
			configLevel = levelName
			writer = opt
		} else if opt != writer {
			return nil, fmt.Errorf("different writer configuration: %s at level %s != %s at level %s", writer, configLevel, opt, levelName)
		}
	}

	if writer, err = c.configure(ctx, configLevel, plan, writer, plan.Levels[configLevel].Collect[DOMAIN].Options); err != nil {
		return nil, err
	}

	for _, levelName := range levels {
		options := plan.Levels[levelName].Collect[DOMAIN].Options
		c.lagWriterIn[levelName] = writer // collect at this level
		c.dropNoHeartbeat[levelName] = !blip.Bool(options[OPT_REPORT_NO_HEARTBEAT])
		c.dropNotAReplica[levelName] = !blip.Bool(options[OPT_REPORT_NOT_A_REPLICA])
		c.defaultChannelNameOverrides[levelName] = options[OPT_DEFAULT_CHANNEL_NAME]
	}

	return c.stopReader, nil
}

// configure sets up the writer from the options of one level. It returns the
// writer, which is never auto.
func (c *Lag) configure(ctx context.Context, levelName string, plan blip.Plan, writer string, options map[string]string) (string, error) {
	blip.Debug("repl.lag: config from level %s", levelName)
	switch writer {
	case LAG_WRITER_PFS:
		// Try collecting, discard metrics
		if _, err := c.collectPFS(ctx, levelName); err != nil {
			return "", err
		}
	case LAG_WRITER_BLIP:
		c.prepareBlip(levelName, plan.MonitorId, plan.Name, options)
	default: // auto
		// Try PFS first, then Blip heartbeat
		if _, err := c.collectPFS(ctx, levelName); err == nil {
			blip.Debug("repl.lag auto-detected PFS")
			writer = LAG_WRITER_PFS
		} else {
			c.prepareBlip(levelName, plan.MonitorId, plan.Name, options)
			blip.Debug("repl.lag auto-detected Blip heartbeat")
			writer = LAG_WRITER_BLIP
		}
	}

	if err := c.verifyReplCheck(ctx, options[OPT_REPL_CHECK]); err != nil {
		return "", err
	}
	c.replCheck = options[OPT_REPL_CHECK]
	return writer, nil
}

func (c *Lag) verifyReplCheck(ctx context.Context, variable string) error {
	if variable == "" {
		return nil
	}

	row := c.db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE ?", variable)
	var value string
	if err := row.Scan(&value, &value); err != nil {
		return fmt.Errorf("failed to verify replication check variable %s: %s", variable, err)
	}
	return nil
}

func (c *Lag) Collect(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	switch c.lagWriterIn[levelName] {
	case LAG_WRITER_BLIP:
		return c.collectBlip(ctx, levelName)
	case LAG_WRITER_PFS:
		return c.collectPFS(ctx, levelName)
	}

	panic(fmt.Sprintf("invalid lag writer in Collect %q in level %q. All levels: %v", c.lagWriterIn[levelName], levelName, c.lagWriterIn))
}

// //////////////////////////////////////////////////////////////////////////
// Internal methods
// //////////////////////////////////////////////////////////////////////////

// prepareBlip starts the heartbeat reader. Only 1 reader per collector.
func (c *Lag) prepareBlip(levelName string, monitorID string, planName string, options map[string]string) {
	if c.lagReader != nil {
		return
	}

	table := options[OPT_HEARTBEAT_TABLE]
	if table == "" {
		table = blip.DEFAULT_HEARTBEAT_TABLE
	}
	netLatency := 50 * time.Millisecond
	if s, ok := options[OPT_NETWORK_LATENCY]; ok {
		n, err := strconv.Atoi(s)
		if err != nil {
			blip.Debug("%s: invalid network-latency: %s: %s (ignoring; using default 50 ms)", monitorID, s, err)
		} else {
			netLatency = time.Duration(n) * time.Millisecond
		}
	}
	c.lagReader = c.newReader(heartbeat.BlipReaderArgs{
		MonitorId:  monitorID,
		DB:         c.db,
		Table:      table,
		SourceId:   options[OPT_HEARTBEAT_SOURCE_ID],
		SourceRole: options[OPT_HEARTBEAT_SOURCE_ROLE],
		ReplCheck:  options[OPT_REPL_CHECK], // not c.replCheck: it's set after the reader starts
		Waiter: heartbeat.SlowFastWaiter{
			MonitorId:      monitorID,
			NetworkLatency: netLatency,
		},
	})
	go c.lagReader.Start()
	blip.Debug("%s: started reader: %s/%s (network latency: %s)", monitorID, planName, levelName, netLatency)
	c.stopReader = func() {
		blip.Debug("%s: stopping reader", monitorID)
		c.lagReader.Stop()
	}
}

func (c *Lag) collectBlip(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	lag, err := c.lagReader.Lag(ctx)
	if err != nil {
		return nil, err
	}
	if !lag.Replica {
		if c.dropNotAReplica[levelName] {
			return nil, nil
		}
	} else if lag.Milliseconds == -1 && c.dropNoHeartbeat[levelName] {
		return nil, nil
	}
	m := blip.MetricValue{
		Name:  "current",
		Type:  blip.GAUGE,
		Value: float64(lag.Milliseconds),
		Meta:  map[string]string{"source": lag.SourceId},
	}
	return []blip.MetricValue{m}, nil
}
