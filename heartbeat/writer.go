// Copyright 2024 Block, Inc.

package heartbeat

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
	"github.com/cashapp/blip/v2/status"
)

const BLIP_TABLE_DDL = `CREATE TABLE IF NOT EXISTS heartbeat (
  src_id   varchar(200)      NOT NULL PRIMARY KEY,
  src_role varchar(200)          NULL DEFAULT NULL,
  ts       timestamp(3)      NOT NULL,  -- heartbeat
  freq     smallint unsigned NOT NULL   -- milliseconds
) ENGINE=InnoDB`

// WriteTimeout is how long to wait for MySQL to execute any heartbeat write.
// This should be much greater than the write frequency (config.heartbeat.freq)
// because it allows for slow network, MySQL, and so on.
var WriteTimeout = 5 * time.Second

// InitErrorWait is how long to wait between retries when initializing the
// heartbeat table (the first INSERT). This should be a relatively long wait.
var InitErrorWait = 10 * time.Second

// ReadOnlyWait is how long to wait when MySQL is read-only (not writable).
// This should be a long wait because it could mean Blip is effectively in
// standby mode on a read-only replica until it's promoted to be the writable
// source, which might not happen for a very long time.
var ReadOnlyWait = 20 * time.Second

type Writer struct {
	monitorId string
	db        *sql.DB
	srcId     string
	srcRole   string
	freq      time.Duration
	table     string
}

func NewWriter(monitorId string, db *sql.DB, cfg blip.ConfigHeartbeat) *Writer {
	if cfg.Freq == "" {
		panic("heartbeat.NewWriter called but config.heartbeat.freq not set")
	}
	if cfg.Table == "" {
		panic("heartbeat.NewWriter called but config.heartbeat.table not set")
	}

	freq, _ := time.ParseDuration(cfg.Freq)

	srcId := cfg.SourceId
	if srcId == "" {
		srcId = monitorId
	}

	return &Writer{
		monitorId: monitorId,
		db:        db,
		srcId:     srcId,
		srcRole:   cfg.Role,
		freq:      freq,
		table:     sqlutil.SanitizeTable(cfg.Table, blip.DEFAULT_DATABASE),
	}
}

func (w *Writer) Write(stopChan, doneChan chan struct{}) error {
	defer close(doneChan)
	defer status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "stopped")

	var (
		err    error
		ctx    context.Context
		cancel context.CancelFunc
	)

	// First INSERT: either creates row if it doesn't exist for this monitor ID,
	// or it updates an existing row with the current timestamp and frequency.
	// This must be done else the simpler UPDATE statements below, which is the
	// real heartbeat, will fail because there's no match row.
	var ping string
	var params []interface{}
	if w.srcRole != "" {
		ping = fmt.Sprintf("INSERT INTO %s (src_id, src_role, ts, freq) VALUES (?, ?, NOW(3), ?) ON DUPLICATE KEY UPDATE ts=NOW(3), freq=?, src_role=?", w.table)
		params = []interface{}{w.srcId, w.srcRole, w.freq.Milliseconds(), w.freq.Milliseconds(), w.srcRole}
	} else {
		ping = fmt.Sprintf("INSERT INTO %s (src_id, src_role, ts, freq) VALUES (?, NULL, NOW(3), ?) ON DUPLICATE KEY UPDATE ts=NOW(3), freq=?, src_role=NULL", w.table)
		params = []interface{}{w.monitorId, w.freq.Milliseconds(), w.freq.Milliseconds()}
	}
	blip.Debug("%s: first heartbeat: %s", w.monitorId, ping)
	for {
		status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "first insert")
		ctx, cancel = context.WithTimeout(context.Background(), WriteTimeout)
		_, err = w.db.ExecContext(ctx, ping, params...)
		cancel()
		if err == nil { // success
			status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "sleep")
			break
		}

		// Error --
		blip.Debug("%s: first heartbeat failed: %s", w.monitorId, err)
		if sqlutil.ReadOnly(err) {
			status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "init: MySQL is read-only, sleeping %s", ReadOnlyWait)
			time.Sleep(ReadOnlyWait)
		} else {
			status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "init: error: %s (sleeping %s)", err, InitErrorWait)
			time.Sleep(InitErrorWait)
		}

		// Was Stop called?
		select {
		case <-stopChan: // yes, return immediately
			return nil
		default: // no
		}
	}

	// ----------------------------------------------------------------------
	// Write heartbeats

	// This is the critical loop, so we use a query literal, not SQL ? params,
	// to void 2 wasted round trips: prep (waste), exec, close (waste).
	// This risk of SQL injection is miniscule because both table and monitorId
	// are sanitized, and Blip should only have write privs on its heartbeat table.
	ping = fmt.Sprintf("UPDATE %s SET ts=NOW(3) WHERE src_id=?", w.table)
	blip.Debug("%s: heartbeat: %s", w.monitorId, ping)
	for {
		time.Sleep(w.freq)

		status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "write")
		ctx, cancel = context.WithTimeout(context.Background(), WriteTimeout)
		_, err = w.db.ExecContext(ctx, ping, w.srcId)
		cancel()
		if err != nil {
			blip.Debug("%s: %s", w.monitorId, err.Error())
			if sqlutil.ReadOnly(err) {
				status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "MySQL is read-only, sleeping %s", ReadOnlyWait)
				time.Sleep(ReadOnlyWait)
			} else {
				status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "write error: %s", err)
				// No special sleep on random errors; keep trying to write at freq
			}
		} else {
			// Set status on successful Exec here, not before Sleep, so it
			// doesn't overwrite status set on Exec error; "sleep" = "write OK"
			status.Monitor(w.monitorId, status.HEARTBEAT_WRITER, "sleep")
		}

		// Was Stop called?
		select {
		case <-stopChan: // yes, return immediately
			return nil
		default: // no
		}
	}
}
