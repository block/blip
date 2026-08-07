// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/cashapp/blip/v2"
)

// This file calculates Replica Lag from Performance Schema
// See collectPFS() for how this is used.
const mySQL8LagQuery = `SELECT
  r.CHANNEL_NAME,
  r.LAST_QUEUED_TRANSACTION,
  r.SERVICE_STATE 'io_thd',
  c.SERVICE_STATE 'sql_thd',
  c.LAST_PROCESSED_TRANSACTION,
  w.WORKER_ID,
  w.LAST_APPLIED_TRANSACTION,
  UNIX_TIMESTAMP(NOW(6)) 'now',
  UNIX_TIMESTAMP(LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP) 'last_applied_ts',
  COALESCE(TIMESTAMPDIFF(MICROSECOND, LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP), 0) 'last_applied_lag',
  UNIX_TIMESTAMP(APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP) 'applying_ts'
FROM
  performance_schema.replication_connection_status r
  JOIN performance_schema.replication_applier_status_by_coordinator c USING (channel_name)
  JOIN performance_schema.replication_applier_status_by_worker w USING (channel_name);
`

// worker is one row from the query above. All timestamps are microseconds from MySQL.
type worker struct {
	channel        string // key
	lastQueuedTrx  string
	ioThd          string
	sqlThd         string
	lastProcTrx    string
	id             int
	lastAppliedTrx string
	now            float64 // from MySQL, microseconds
	lastAppliedTs  float64
	lastAppliedLag float64
	applyingTs     float64
}

// pfsLag is computed from a []worker per channel.
type pfsLag struct {
	applying    uint    // how many workers are applying
	observed    string  // O_ const (just for print, human observation)
	current     float64 // milliseconds
	trxId       string  // max applied trx ID (just for print, human observation)
	backlog     int     // last queued - last applied
	workerUsage float64 // applying workers / total workers * 100
}

const (
	O_APPLYING = "a" // at least 1 worker applying
	O_RECEIVED = "r" // no workers applying but more trx queued
	O_STOPPED  = "x" // IO or SQL thread != "ON"
	O_IDLE     = " " // none of the above == true zero lag
)

func (c *Lag) collectPFS(ctx context.Context, levelName string) ([]blip.MetricValue, error) {
	var defaultLag []blip.MetricValue
	if c.dropNotAReplica[levelName] {
		defaultLag = nil
	} else {
		// send -1 for lag
		m := blip.MetricValue{
			Name:  "current",
			Type:  blip.GAUGE,
			Value: float64(-1),
		}
		defaultLag = []blip.MetricValue{m}
	}

	// if isReplCheck is supplied, check if it's a replica
	isRepl := 1
	if c.replCheck != "" {
		query := "SELECT @@" + c.replCheck
		if err := c.db.QueryRowContext(ctx, query).Scan(&isRepl); err != nil {
			return nil, fmt.Errorf("checking if instance is replica failed, please check value of %s. Err: %s", OPT_REPL_CHECK, err.Error())
		}
	}

	if isRepl == 0 {
		return defaultLag, nil
	}

	rows, err := c.db.QueryContext(context.Background(), mySQL8LagQuery)
	if err != nil {
		return nil, fmt.Errorf("could not check replication lag, check that the host is a MySQL 8.0 replica, and that performance_schema is enabled. Err: %s", err.Error())
	}

	// Group workers by channel name
	channels := map[string][]worker{}
	for rows.Next() {
		w := worker{}
		if err := rows.Scan(&w.channel, &w.lastQueuedTrx, &w.ioThd, &w.sqlThd, &w.lastProcTrx, &w.id, &w.lastAppliedTrx, &w.now, &w.lastAppliedTs, &w.lastAppliedLag, &w.applyingTs); err != nil {
			log.Fatal(err)
		}
		if _, ok := channels[w.channel]; !ok { // new channel
			channels[w.channel] = []worker{}
		}
		channels[w.channel] = append(channels[w.channel], w)
	}
	rows.Close()

	var lagMetrics []blip.MetricValue
	// collect lag per channel
	for channel, workers := range channels {
		// MySQL use "" as the default channel name, blip provides a way to override it
		if channel == "" && c.defaultChannelNameOverrides[levelName] != "" {
			channel = c.defaultChannelNameOverrides[levelName]
		}
		lag := lagFor(workers, c.pfsLagLastQueued, c.pfsLagLastProc)
		lagMetrics = append(lagMetrics, blip.MetricValue{
			Name:  "current",
			Type:  blip.GAUGE,
			Group: map[string]string{"channel": channel},
			Value: lag.current,
		})
		lagMetrics = append(lagMetrics, blip.MetricValue{
			Name:  "backlog",
			Type:  blip.GAUGE,
			Value: float64(lag.backlog),
			Group: map[string]string{"channel": channel},
		})
		lagMetrics = append(lagMetrics, blip.MetricValue{
			Name:  "worker_usage",
			Type:  blip.GAUGE,
			Value: lag.workerUsage,
			Group: map[string]string{"channel": channel},
		})
		blip.Debug("(repl.lag from PFS): channel: %s txID: %s Observed State: %s Num of applying workers: %d | backlog: %3d worker Usage: %3.2f%% lag=%d ms", channel, lag.trxId, lag.observed, lag.applying, lag.backlog, lag.workerUsage, int(lag.current))
	}
	return lagMetrics, nil
}

func lagFor(workers []worker, lastQueued, lastProc map[string]string) pfsLag {
	lag := pfsLag{}               // return value
	channel := workers[0].channel // for brevity
	maxTrxNo := 0                 //  backlog = last queued trxNo - maxTrxNo
	var (
		oldestApplyingTs float64
		lastAppliedTs    float64
		lastAppliedLag   float64
	)

	for _, w := range workers {
		// Count workers that are applying and save the oldest (longest running)
		// one because we report worst case lag. For example, given trx set 1-10,
		// if worker1 is applying trx 5, and worker2 is applying trx 9, lag is
		// 10-5=5 not 10-9=1.
		if w.applyingTs > 0 { // worker is applying
			lag.applying += 1
			if w.applyingTs < oldestApplyingTs || lag.applying == 1 {
				oldestApplyingTs = w.applyingTs
			}
		}

		// Save max worker trx number (GTID). Later, if no workers are applying,
		// this is point at which the replica has caught up. It's also used to
		// calculate the backlog (optimistically presuming any gaps will be applied
		// successfully).
		n := trxNo(w.lastAppliedTrx)
		if n > maxTrxNo {
			maxTrxNo = n
			lastAppliedTs = w.lastAppliedTs
			lastAppliedLag = w.lastAppliedLag
			lag.trxId = w.lastAppliedTrx
		}

		//
		// If-else order matters here! Read the comments top to bottom.
		//
		// NOTE: ts are seconds (e.g. 1716922205.641072) so multiply by 1000
		// for milliseconds. But the one lag value, lastAppliedLag, is microseconds,
		// so divide by 1000 for milliseconds.
		//
		if lag.applying > 0 {
			// At least one worker is applying, so report worst case applying lag.
			// See comments above re oldestApplyingTs. Ignore applied trx/lag and
			// thread state for now because as long as a worker/trx is applying,
			// it's a reliable "signal" of lag, like receiving a heartbeat. If the
			// replica is actually dead, eventually no workers will be applying and
			// one of the if-else cases below will be true.
			lag.current = math.Floor((workers[0].now - oldestApplyingTs) * 1000.0) // as milliseconds
			lag.observed = O_APPLYING

		} else if workers[0].lastQueuedTrx != lastQueued[channel] {
			// No workers are applying (all are idle) but the replica received new
			// trx since last time we looked. This means replication is working.
			// If we report lag = NOW - last applied ts, then we introduce an artifact:
			// polling time. If polling every 5s and the last applied trx happened
			// 4s ago, it'll look like 4s of lag even if the trx lagged only 100ms.
			// Since repl events arrive randomly (they're not fixed interval heartbeats)
			// and Blip can be configured to poll (collect repl.lag) at any interval,
			// this won't work well. Instead, we know only one thing for sure:
			//   last applied lag =
			//     LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP - LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP
			// Those column values come from the max worker trx number (see comments
			// above), so it's the lag of the last applied trx as measured from
			// source to replica, irrespective of polling frequency or current time.
			// In the example above, this trx might have lagged only 100ms, and then
			// we observe it 4.9s later but correctly report 100ms. -- This is a bit
			// of an edge case where appliers do work in between observations,
			// probably because replica isn't super busy so trx are quick and
			// intermittent. Reporting zero would be misleading now that we can
			// look back and see objective lag recorded by MySQL in these tables.
			lag.current = math.Floor(lastAppliedLag / 1000.0) // as milliseconds
			lag.observed = O_RECEIVED

		} else if workers[0].ioThd != "ON" || workers[0].sqlThd != "ON" {
			// No workers applying and no trx received. If either repl thread is
			// not in a known good state, then we err on the side of caution and
			// report increasing lag from the time of the last applied trx.
			// This has become industry standard practice: a stopped replica means
			// lag increases. If someone isn't monitoring thread state, this might
			// be the only way they detect that the replica has stopped (presuming
			// they're monitoring/alerting on high repl lag).
			lag.current = math.Floor((workers[0].now - lastAppliedTs) * 1000.0) // as milliseconds
			lag.observed = O_STOPPED

		} else {
			// Not applying, not received any trx, and threads are both ok:
			// this is true zero repl lag. The source or network might be having
			// an issue, but those are outside the replica, so not something we
			// can reliably measure or report. From the replica point of view,
			// there is/was truly no work since we last looked. -- Since we
			// presume the replica should be busy, this can (and should) be
			// used for an alert: if lag = 0 for 5 minutes -> alert.
			lag.current = 0
			lag.observed = O_IDLE
		}
	}

	lag.backlog = trxNo(workers[0].lastQueuedTrx) - maxTrxNo
	lag.workerUsage = float64(lag.applying) / float64(len(workers)) * 100

	// Save observed trx for calculations in next call. All workers have
	// same value (because they come from repl conn status and coordinator
	// tables) so workers[0] is used.
	lastQueued[channel] = workers[0].lastQueuedTrx
	lastProc[channel] = workers[0].lastProcTrx

	return lag
}

// trxNo takes a GTID like a930bd5c-1e21-11ef-a9a6-0242ac1c000a:1234 and returns 1234.
func trxNo(gtid string) int {
	if gtid == "" {
		return 0
	}
	trxNo, err := strconv.Atoi(gtid[strings.IndexRune(gtid, ':')+1:])
	if err != nil {
		panic("invalid GTID: " + gtid)
	}
	return trxNo
}
