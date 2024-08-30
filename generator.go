package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync/atomic"
)

type TableGen struct {
	x    *Rows
	t    Table
	rows int64
}

func (t TableGen) Arguments() []any {
	return nil
}

func (t TableGen) NamedArguments() map[string]any {
	return nil
}

type TableGenBind struct {
	p *TableGen
}
type tblScan struct {
	ch   Chunk
	tbl  *TableGen
	proj []int
	fns  []func(src int, dst *Vector)
}

func (t *tblScan) Scan(chunk *UDFDataChunk) (int, error) {
	if r := atomic.AddInt64(&t.tbl.rows, -1); r <= 0 {
		return 0, nil
	}
	err := t.tbl.x.NextChunk(&t.ch)
	if err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	for i, f := range t.fns {
		f(t.proj[i], &chunk.Columns[i])
	}

	defer t.ch.Close()

	return t.ch.NumValues(), err
}

func (t *tblScan) Close() {
	t.ch.Close()
}

func (t *TableGenBind) Table() *Table {
	return &t.p.t
}

func (t *TableGenBind) InitScanner(vecSize int, projection []int) Scanner {
	sc := &tblScan{tbl: t.p, proj: projection}

	table := t.Table()
	fmt.Println("TYPES", table.Columns, projection)
	for _, src := range projection {

		var fn func(src int, dst *Vector)
		switch table.Columns[src].Type {
		case FLOAT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Float32(src)
				AppendMany(dst, v)
			}
		case DOUBLE:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Float64(src)
				AppendMany(dst, v)
			}
		case BIGINT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Int64(src)
				AppendMany(dst, v)
			}
		case UBIGINT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Uint64(src)
				AppendMany(dst, v)
			}
		case INTEGER:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Int32(src)
				AppendMany(dst, v)
			}
		case UINTEGER:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Uint32(src)
				AppendMany(dst, v)
			}
		case SMALLINT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Int16(src)
				AppendMany(dst, v)
			}
		case USMALLINT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Uint16(src)
				AppendMany(dst, v)
			}
		case TINYINT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Int8(src)
				AppendMany(dst, v)
			}
		case UTINYINT:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Uint8(src)
				AppendMany(dst, v)
			}
		case UUIDTYP:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.UUID(src)
				AppendMany(dst, v)
			}
		case VARCHAR:
			fn = func(src int, dst *Vector) {
				v, _ := sc.ch.Varchar(src)
				for _, r := range v {
					AppendBytes(dst, r.Bytes())
				}
			}
		default:
			panic("unsupported type")
		}
		sc.fns = append(sc.fns, fn)
	}

	return sc
}

func (t *TableGen) Bind(named map[string]any, args []any) (Binding, error) {
	return &TableGenBind{p: t}, nil
}

func WithTableGenerator(q string, fn func(driver.Conn), maxRows int) error {
	var ctx = context.Background()

	conn, err := NewConnector("/var/tmp/mytest.duckdb", nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	co, err := conn.ConnectRaw(ctx)
	if err != nil {
		return err
	}
	defer co.Close()

	co2, err := conn.ConnectRaw(ctx)
	if err != nil {
		return err
	}
	defer co2.Close()

	fn(co2)

	rows, err := co.ExtendedQueryContext(ctx, q, nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	var cols = rows.Columns()
	var tbl = Table{MaxThreads: runtime.NumCPU()}

	for i := range cols {
		tbl.Columns = append(tbl.Columns, ColumnDef{Name: cols[i], Type: rows.ColumnTypeDatabaseTypeName(i)})
	}
	ff := &TableGen{x: rows, t: tbl}

	if err := RegisterTableUDFConn(co2, "tablegen", ff); err != nil {
		return err
	}

	for i := 0; ff.x.Err() == nil; i++ {
		ff.rows = int64(maxRows / 2048)
		if i == 0 {
			_, err := co2.Exec(fmt.Sprintf("drop table if exists x;"))
			if err != nil {
				return err
			} else if _, err := co2.Exec(fmt.Sprintf(" create or replace table x as from tablegen() order by 1")); err != nil {
				return err
			}
		} else {
			if _, err := co2.Exec(fmt.Sprintf(" insert into x from tablegen() order by 1")); err != nil {
				return err
			}
		}

	}
	log.Println("DONE!")
	if err := ff.x.Err(); err == io.EOF {
		return nil
	} else {
		return err
	}
}
