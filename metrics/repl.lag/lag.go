// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"database/sql"
	"fmt"
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
// The first level sets up the writer. Every level must use the same writer,
// but the report-no-heartbeat, report-not-a-replica, and default-channel-name
// options are per level.
//
// If lag is collected from a Blip heartbeat, the returned cleanup func stops
// the heartbeat reader; else it's nil. On error, Prepare stops the reader
// and returns a nil cleanup func, as required by blip.Collector.
func (c *Lag) Prepare(ctx context.Context, plan blip.Plan) (cleanup func(), err error) {
	configured := "" // writer option of first level, set after it's configured
	writer := ""     // writer that configured resolved to: pfs or blip

	// The engine discards this collector if Prepare fails, so stop the
	// reader if it was started before the error.
	defer func() {
		if err != nil && c.stopReader != nil {
			c.stopReader()
		}
	}()

LEVEL:
	for levelName, level := range plan.Levels {
		dom, ok := level.Collect[DOMAIN]
		if !ok {
			continue LEVEL // not collected in this level
		}

		opt := dom.Options[OPT_WRITER]
		if opt == "" {
			opt = "auto" // default
		}

		// Set up the writer at the first level. At other levels, the same writer
		// is ok and expected (lag collected at multiple levels). But if writer
		// is different, that's an error.
		if configured == "" {
			if writer, err = c.configure(ctx, levelName, plan, opt, dom.Options); err != nil {
				return nil, err
			}
			configured = opt
		} else if opt != configured {
			return nil, fmt.Errorf("different writer configuration: %s != %s", configured, opt)
		}

		c.lagWriterIn[levelName] = writer // collect at this level
		c.dropNoHeartbeat[levelName] = !blip.Bool(dom.Options[OPT_REPORT_NO_HEARTBEAT])
		c.dropNotAReplica[levelName] = !blip.Bool(dom.Options[OPT_REPORT_NOT_A_REPLICA])
		c.defaultChannelNameOverrides[levelName] = dom.Options[OPT_DEFAULT_CHANNEL_NAME]
	}

	return c.stopReader, nil
}

// configure sets up the writer from the options of the first level that
// collects lag. It returns the writer, which is never auto.
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
	case "auto":
		// Try PFS first, then Blip heartbeat
		if _, err := c.collectPFS(ctx, levelName); err == nil {
			blip.Debug("repl.lag auto-detected PFS")
			writer = LAG_WRITER_PFS
		} else {
			c.prepareBlip(levelName, plan.MonitorId, plan.Name, options)
			blip.Debug("repl.lag auto-detected Blip heartbeat")
			writer = LAG_WRITER_BLIP
		}
	default:
		return "", fmt.Errorf("invalid lag writer: %q; valid values: auto, pfs, blip", writer)
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
