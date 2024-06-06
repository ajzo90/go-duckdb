package duckdb

import (
	"context"
	"database/sql/driver"
)

func (c *conn) PrepareContext(ctx context.Context, cmd string) (*stmt, error) {
	s, err := c.Prepare(cmd)
	return s.(*stmt), err
}

type Conn struct {
	conn
}

type Rows struct {
	rows
}

func (c *Connector) ConnectRaw(ctx context.Context) (*Conn, error) {
	con, err := c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: *con.(*conn)}, err
}
func (s *stmt) QueryContextRaw(ctx context.Context, args []driver.NamedValue) (*Rows, error) {
	r, err := s.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}

	return &Rows{rows: *(r.(*rows))}, nil
}

func getConn(c any) (*conn, error) {
	var duckConn *conn
	if co, ok := c.(*conn); ok {
		duckConn = co
	} else if co, ok := c.(*Conn); ok {
		duckConn = &co.conn
	} else {
		return nil, driver.ErrBadConn
	}
	return duckConn, nil
}
