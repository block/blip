// Copyright 2026 Block, Inc.

package test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
)

const pingDriverName = "blip-test-ping"

var registerPingDriver sync.Once

// PingDB returns a database handle that doesn't need MySQL: pings succeed and
// queries fail. It's for tests that need a database only to connect, like
// monitor.Engine.Prepare.
func PingDB() (*sql.DB, error) {
	registerPingDriver.Do(func() { sql.Register(pingDriverName, pingDriver{}) })
	return sql.Open(pingDriverName, "")
}

var errPingDB = errors.New("test.PingDB: queries not supported")

type pingDriver struct{}

func (pingDriver) Open(string) (driver.Conn, error) { return pingConn{}, nil }

type pingConn struct{}

var _ driver.Pinger = pingConn{}

func (pingConn) Ping(context.Context) error          { return nil }
func (pingConn) Prepare(string) (driver.Stmt, error) { return nil, errPingDB }
func (pingConn) Close() error                        { return nil }
func (pingConn) Begin() (driver.Tx, error)           { return nil, errPingDB }
