// Copyright 2024 Block, Inc.

package mock

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// ErrorDB returns a *sql.DB whose connection attempts always fail with err,
// so every query on it returns err. Use it to test how collectors handle and
// wrap query errors without a running MySQL instance.
func ErrorDB(err error) *sql.DB {
	return sql.OpenDB(errorConnector{err: err})
}

type errorConnector struct {
	err error
}

func (c errorConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, c.err
}

func (c errorConnector) Driver() driver.Driver {
	return errorDriver{err: c.err}
}

type errorDriver struct {
	err error
}

func (d errorDriver) Open(string) (driver.Conn, error) {
	return nil, d.err
}
