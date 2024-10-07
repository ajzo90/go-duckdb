package duckdb

import (
	"context"
	"database/sql/driver"
	"sync"
)

func (c *conn) PrepareContext(ctx context.Context, cmd string) (*stmt, error) {
	s, err := c.Prepare(cmd)
	return s.(*stmt), err
}

func (c *conn) ExtendedQueryContext(ctx context.Context, q string, args []driver.NamedValue) (*Rows, error) {
	r, err := c.QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: r.(*rows)}, nil
}

type Conn struct {
	conn
}

func UpgradeConn(connection Connection) *Conn {
	return &Conn{conn: conn{duckdbCon: connection}}
}

type Rows struct {
	mtx sync.Mutex
	err error
	*rows
}

func (c *Conn) Exec(q string, args ...driver.NamedValue) (driver.Result, error) {
	return c.ExecContext(context.Background(), q, args)
}

func (c *Connector) ConnectRaw(ctx context.Context) (*Conn, error) {
	con, err := c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: *con.(*conn)}, err
}

//func (s *stmt) QueryContextRaw(ctx context.Context, args []driver.NamedValue) (*Rows, error) {
//	r, err := s.QueryContext(ctx, args)
//	if err != nil {
//		return nil, err
//	}
//	return &Rows{rows: r.(*rows)}, nil
//}

func _getConn(c any) (*conn, bool) {
	if co, ok := c.(*conn); ok {
		return co, true
	} else if co, ok := c.(*Conn); ok {
		return &co.conn, true
	} else {
		return nil, false
	}
}

func getConn(c any) (*conn, error) {
	con, ok := _getConn(c)
	if !ok {
		return nil, getError(errAppenderInvalidCon, nil)
	}
	if con.closed {
		return nil, getError(errAppenderClosedCon, nil)
	}
	return con, nil
}
