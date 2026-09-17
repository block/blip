// Copyright 2024 Block, Inc.

package sizedatabase_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/cashapp/blip/v2"
	sizedatabase "github.com/cashapp/blip/v2/metrics/size.database"
)

var errRows = errors.New("row stream failed")

func TestCollectReturnsRowStreamError(t *testing.T) {
	sql.Register("size-database-row-error", rowErrorDriver{})
	db, err := sql.Open("size-database-row-error", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	collector := sizedatabase.NewDatabase(db)
	plan := blip.Plan{Levels: map[string]blip.Level{
		"level": {
			Name: "level",
			Collect: map[string]blip.Domain{
				sizedatabase.DOMAIN: {},
			},
		},
	}}
	if _, err := collector.Prepare(context.Background(), plan); err != nil {
		t.Fatal(err)
	}

	metrics, err := collector.Collect(context.Background(), "level")
	if !errors.Is(err, errRows) {
		t.Fatalf("Collect error = %v, expected %v", err, errRows)
	}
	if metrics != nil {
		t.Fatalf("Collect returned partial metrics: %#v", metrics)
	}
}

type rowErrorDriver struct{}

func (rowErrorDriver) Open(string) (driver.Conn, error) {
	return rowErrorConn{}, nil
}

type rowErrorConn struct{}

func (rowErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare is not supported")
}

func (rowErrorConn) Close() error {
	return nil
}

func (rowErrorConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin is not supported")
}

func (rowErrorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &rowErrorRows{}, nil
}

type rowErrorRows struct {
	nextCall int
}

func (*rowErrorRows) Columns() []string {
	return []string{"db", "bytes"}
}

func (*rowErrorRows) Close() error {
	return nil
}

func (r *rowErrorRows) Next(values []driver.Value) error {
	switch r.nextCall {
	case 0:
		r.nextCall++
		values[0] = "complete_schema"
		values[1] = "42"
		return nil
	case 1:
		r.nextCall++
		return errRows
	default:
		return io.EOF
	}
}
