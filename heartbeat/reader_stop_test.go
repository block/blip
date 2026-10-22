// Copyright 2026 Block, Inc.

package heartbeat

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/blip/v2/status"
)

// blockingDB is a database whose queries block until release is closed (or
// the query context is done), then fail.
type blockingDB struct {
	running chan struct{} // receives when a query starts
	release chan struct{}
}

func newBlockingDB(t *testing.T) (*sql.DB, *blockingDB) {
	b := &blockingDB{running: make(chan struct{}, 1), release: make(chan struct{})}
	db := sql.OpenDB(b)
	t.Cleanup(func() { db.Close() })
	return db, b
}

func (b *blockingDB) Connect(context.Context) (driver.Conn, error) { return blockingConn{b}, nil }
func (b *blockingDB) Driver() driver.Driver                        { return nil }

type blockingConn struct{ b *blockingDB }

func (c blockingConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	select {
	case c.b.running <- struct{}{}:
	default:
	}
	select {
	case <-c.b.release:
	case <-ctx.Done():
	}
	return nil, errors.New("query failed")
}

func (blockingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (blockingConn) Close() error                        { return nil }
func (blockingConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func TestBlipReaderStopWaitsForRunningQuery(t *testing.T) {
	// The monitor loader removes a monitor's status after stopping it. If Stop
	// returned while a heartbeat query was running, the reader would report
	// status after that, which makes the removed monitor reappear.
	monitorId := "stop-waits"
	db, d := newBlockingDB(t)
	r := NewBlipReader(BlipReaderArgs{MonitorId: monitorId, DB: db, Table: "blip.heartbeat"})
	r.Start()
	select {
	case <-d.running:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat query didn't start")
	}

	// Stop while the query is running, which returns after Stop is called
	time.AfterFunc(100*time.Millisecond, func() { close(d.release) })
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ { // concurrent Stop calls
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Stop()
		}()
	}
	wg.Wait()
	status.RemoveMonitor(monitorId)

	<-r.doneChan // reader returned, so it reported any status it was going to
	if got := status.ReportMonitors(monitorId)[monitorId]; len(got) != 0 {
		t.Errorf("monitor status after Stop and RemoveMonitor: %v, expected none", got)
	}
	r.Stop() // again, after it stopped
}

func TestBlipReaderStopBeforeStart(t *testing.T) {
	// Repl.lag calls Start in a goroutine, so Stop can be called first
	db, _ := newBlockingDB(t)
	r := NewBlipReader(BlipReaderArgs{MonitorId: "stop-before-start", DB: db, Table: "blip.heartbeat"})
	stopped := make(chan struct{})
	go func() {
		r.Stop()
		r.Start() // doesn't start a stopped reader
		r.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop blocked for a reader that wasn't started")
	}
	r.Lock()
	started := r.started
	r.Unlock()
	if started {
		t.Error("Start started a stopped reader")
	}
}
