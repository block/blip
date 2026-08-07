// Copyright 2024 Block, Inc.

package heartbeat

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/status"
)

// Reader reads heartbeats from a writer. It runs in a separate goroutine and
// reports replication lag for the repl.lag metric collector, where it's also
// created in Prepare. Currently, there's only one implementation: BlipReader,
// but an implementation for pt-table-heartbeat is an idea.
type Reader interface {
	Start() error
	Stop()
	Lag(context.Context) (Lag, error)
}

type Lag struct {
	Milliseconds int64
	LastTs       time.Time
	SourceId     string
	SourceRole   string
	Replica      bool
}

var (
	ReadTimeout     = 2 * time.Second
	ReadErrorWait   = 1 * time.Second
	NoHeartbeatWait = 3 * time.Second
	ReplCheckWait   = 3 * time.Second
)

// BlipReader reads heartbeats from BlipWriter.
type BlipReader struct {
	monitorId string
	db        *sql.DB
	table     string
	srcId     string
	srcRole   string
	replCheck string
	// --
	waiter LagWaiter
	*sync.Mutex
	lag      int64
	last     time.Time
	stopChan chan struct{}
	doneChan chan struct{}
	isRepl   bool
	event    event.MonitorReceiver
	query    string
	params   []interface{}
}

type BlipReaderArgs struct {
	MonitorId  string
	DB         *sql.DB
	Table      string
	SourceId   string
	SourceRole string
	ReplCheck  string
	Waiter     LagWaiter
}

func NewBlipReader(args BlipReaderArgs) *BlipReader {
	r := &BlipReader{
		monitorId: args.MonitorId,
		db:        args.DB,
		table:     args.Table,
		srcId:     args.SourceId,
		srcRole:   args.SourceRole,
		replCheck: args.ReplCheck,
		// --
		waiter:   args.Waiter,
		Mutex:    &sync.Mutex{},
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		lag:      -1, // no heartbeat
		isRepl:   true,
		event:    event.MonitorReceiver{MonitorId: args.MonitorId},
	}

	// Create heartbeat read query
	cols := []string{"NOW(3)", "ts", "freq", "src_id", "1"}
	var where string
	if r.srcId != "" {
		blip.Debug("%s: heartbeat from source %s", r.monitorId, r.srcId)
		where = "WHERE src_id=?" // default
		r.params = []interface{}{r.srcId}
	} else if r.srcRole != "" {
		blip.Debug("%s: heartbeat from role %s", r.monitorId, r.srcRole)
		where = "WHERE src_role=? ORDER BY ts DESC LIMIT 1"
		r.params = []interface{}{r.srcRole}
	} else {
		blip.Debug("%s: heartbeat from latest (max ts)", r.monitorId)
		where = "WHERE src_id != ? ORDER BY ts DESC LIMIT 1"
		r.params = []interface{}{args.MonitorId}
	}
	if r.replCheck != "" {
		cols[4] = "@@" + r.replCheck
	}
	r.query = fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(cols, ", "), r.table, where)

	return r
}

func (r *BlipReader) Start() error {
	go r.run()
	return nil
}

func (r *BlipReader) run() {
	defer close(r.doneChan)
	blip.Debug("%s: heartbeat reader: %s", r.monitorId, r.query)

	var (
		now    time.Time     // now according to MySQL
		last   sql.NullTime  // last heartbeat
		freq   int           // freq of heartbeats (milliseconds)
		lag    int64         // lag since last
		srcId  string        // source_id, might change if using src_role
		isRepl int           // @@repl-check
		wait   time.Duration // wait time until next check
		err    error
		ctx    context.Context
		cancel context.CancelFunc
	)
	for {
		select {
		case <-r.stopChan:
			return
		default:
		}

		ctx, cancel = context.WithTimeout(context.Background(), ReadTimeout)
		err = r.db.QueryRowContext(ctx, r.query, r.params...).Scan(&now, &last, &freq, &srcId, &isRepl)
		cancel()
		if err != nil {
			blip.Debug("%s: %v", r.monitorId, err)
			switch {
			case err == sql.ErrNoRows:
				r.Lock()
				r.lag = -1 // no heartbeat
				r.Unlock()
				status.Monitor(r.monitorId, "error:"+status.HEARTBEAT_READER, "no heartbeat for %s (retry in %s)", r.srcId, NoHeartbeatWait)
				time.Sleep(NoHeartbeatWait)
			default:
				status.Monitor(r.monitorId, "error:"+status.HEARTBEAT_READER, "error: %s (retry in %s)", err.Error(), ReadErrorWait)
				time.Sleep(ReadErrorWait)
			}
			continue
		}
		status.RemoveComponent(r.monitorId, "error:"+status.HEARTBEAT_READER)

		if isRepl == 0 {
			r.Lock()
			r.isRepl = false
			r.Unlock()
			msg := fmt.Sprintf("not a replica: %s=%d (retry in %s)", r.replCheck, isRepl, ReplCheckWait)
			blip.Debug("%s: %s", r.monitorId, msg)
			status.Monitor(r.monitorId, status.HEARTBEAT_READER, "%s", msg)
			time.Sleep(ReplCheckWait)
			continue
		}

		// Repl source channge?
		if r.srcId != srcId {
			r.event.Sendf(event.REPL_SOURCE_CHANGE, "%s to %s", r.srcId, srcId)
			r.srcId = srcId
		}

		lag, wait = r.waiter.Wait(now, last.Time, freq, srcId)

		r.Lock()
		r.isRepl = true
		r.lag = lag
		r.last = last.Time
		r.Unlock()

		status.Monitor(r.monitorId, status.HEARTBEAT_READER, "%d ms lag from %s (%s), next in %s", lag, srcId, r.srcRole, wait)
		time.Sleep(wait)
	}
}

func (r *BlipReader) Stop() {
	r.Lock()
	select {
	case <-r.stopChan:
	case <-r.doneChan:
	default:
		close(r.stopChan)
	}
	r.Unlock()
}

func (r *BlipReader) Lag(_ context.Context) (Lag, error) {
	r.Lock()
	defer r.Unlock()
	if !r.isRepl {
		return Lag{Replica: false, Milliseconds: -1}, nil
	}
	return Lag{Milliseconds: r.lag, LastTs: r.last, SourceId: r.srcId, SourceRole: r.srcRole, Replica: true}, nil
}

// --------------------------------------------------------------------------

type LagWaiter interface {
	Wait(now, past time.Time, freq int, srcId string) (int64, time.Duration)
}

type SlowFastWaiter struct {
	MonitorId      string
	NetworkLatency time.Duration
}

var _ LagWaiter = SlowFastWaiter{}

func (w SlowFastWaiter) Wait(now, last time.Time, freq int, srcId string) (int64, time.Duration) {
	next := last.Add(time.Duration(freq) * time.Millisecond)
	blip.Debug("%s: last=%s  now=%s  next=%s  src=%s", w.MonitorId, last, now, next, srcId)

	if now.Before(next) {
		lag := now.Sub(last) - w.NetworkLatency
		if lag < 0 {
			lag = 0
		}

		// Wait until next hb
		d := next.Sub(now) + w.NetworkLatency
		blip.Debug("%s: lagged: %d ms; next hb in %d ms", w.MonitorId, lag.Milliseconds(), next.Sub(now).Milliseconds())
		return lag.Milliseconds(), d
	}

	// Next hb is late (lagging)
	lag := now.Sub(next).Milliseconds()
	var wait time.Duration
	switch {
	case lag < 200:
		wait = time.Duration(50 * time.Millisecond)
		break
	case lag < 600:
		wait = time.Duration(100 * time.Millisecond)
		break
	case lag < 2000:
		wait = time.Duration(500 * time.Millisecond)
		break
	default:
		wait = time.Second
	}

	blip.Debug("%s: lagging: %s; wait %s", w.MonitorId, now.Sub(next), wait)
	return lag, wait
}
