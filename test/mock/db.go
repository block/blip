// Copyright 2026 Block, Inc.

package mock

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

// ContextDB returns a *sql.DB for testing how collectors handle a query
// context that ends, without a running MySQL instance. Queries return rows
// with the given columns, but reading the rows blocks until the query context
// is done, then fails with its error, like a read that the collector max
// runtime (CMR) stops. A query with a context that is already done fails
// before it runs, like a query started after the CMR.
func ContextDB(columns ...string) *sql.DB {
	return sql.OpenDB(contextConnector{columns: columns})
}

var errNotSupported = errors.New("mock.ContextDB: not supported")

type contextConnector struct {
	columns []string
}

func (c contextConnector) Connect(context.Context) (driver.Conn, error) {
	return contextConn{columns: c.columns}, nil
}

func (c contextConnector) Driver() driver.Driver {
	return contextDriver{}
}

type contextDriver struct{}

func (contextDriver) Open(string) (driver.Conn, error) {
	return nil, errNotSupported
}

type contextConn struct {
	columns []string
}

var _ driver.QueryerContext = contextConn{}

func (c contextConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return contextRows{ctx: ctx, columns: c.columns}, nil
}

func (contextConn) Prepare(string) (driver.Stmt, error) {
	return nil, errNotSupported
}

func (contextConn) Close() error {
	return nil
}

func (contextConn) Begin() (driver.Tx, error) {
	return nil, errNotSupported
}

type contextRows struct {
	ctx     context.Context
	columns []string
}

func (r contextRows) Columns() []string {
	return r.columns
}

func (contextRows) Close() error {
	return nil
}

func (r contextRows) Next([]driver.Value) error {
	<-r.ctx.Done()
	return r.ctx.Err()
}
