package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
)

type schema struct {
	schema *duckdb.Schema
	rows   int64
	strVal string
}

type myTableUDF struct {
	schemas     map[duckdb.Ref]*schema
	localStates map[duckdb.Ref]*udfScanner
	mtx         sync.RWMutex
}

func newMyTableUDF() *myTableUDF {
	return &myTableUDF{
		schemas:     map[duckdb.Ref]*schema{},
		localStates: map[duckdb.Ref]*udfScanner{},
	}
}

func (d *myTableUDF) GetArguments() []any {
	return []any{
		int64(0),
		any(""),
	}
}

func (d *myTableUDF) BindArguments(args ...any) (schem duckdb.Ref) {
	var rows = args[0].(int64)
	var strVal = args[1].(string)
	schema := &schema{
		rows:   rows,
		strVal: strVal,
		schema: &duckdb.Schema{
			Columns: []duckdb.ColumnDef{
				{"result", int64(0)},
				{"xxx", ""},
			},
			Cardinality:      int(rows),
			ExactCardinality: false,
			MaxThreads:       12,
		},
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()

	ref := duckdb.Ref(len(d.schemas))
	d.schemas[ref] = schema
	return ref
}

func (d *myTableUDF) GetSchema(ref duckdb.Ref) *duckdb.Schema {
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	return d.schemas[ref].schema
}

type udfScanner struct {
	schema  *schema
	vecSize int
	scr     []byte
	fns     []func(vector *duckdb.Vector)
}

func (l *udfScanner) Scan(ch *duckdb.DataChunk) int {
	rem := atomic.AddInt64(&l.schema.rows, -int64(l.vecSize))
	if rem < 0 {
		l.vecSize += int(rem)
		if l.vecSize < 0 {
			l.vecSize = 0
		}
	}

	for i, f := range l.fns {
		f(&ch.Columns[i])
	}

	return l.vecSize
}

func (d *myTableUDF) GetScanner(ref duckdb.Ref) duckdb.Scanner {
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	return d.localStates[ref]
}

func (d *myTableUDF) InitScanner(parent duckdb.Ref, vecSize int) (scanner duckdb.Ref) {

	schema := d.schemas[parent]
	s := &udfScanner{
		schema:  schema,
		vecSize: vecSize,
		fns:     make([]func(vector *duckdb.Vector), 0, len(schema.schema.Projection)),
	}
	for _, pos := range schema.schema.Projection {
		var fn func(vec *duckdb.Vector)
		switch pos {
		case 0:
			fn = func(vec *duckdb.Vector) {
				for i := 0; i < s.vecSize; i++ {
					vec.AppendInt64(int64(i % 10))
				}
			}
		case 1:
			fn = func(vec *duckdb.Vector) {
				for i := 0; i < s.vecSize; i++ {
					s.scr = append(s.scr[:0], schema.strVal...)
					vec.AppendBytes(s.scr)
				}
			}
		}
		s.fns = append(s.fns, fn)
	}

	d.mtx.Lock()
	defer d.mtx.Unlock()

	ref := duckdb.Ref(len(d.localStates))
	d.localStates[ref] = s
	return ref
}

func main() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())

	check(duckdb.RegisterTableUDF(conn, "udf", duckdb.UDFOptions{ProjectionPushdown: true}, newMyTableUDF()))
	check(db.Ping())

	const q = `SELECT xxx, count(*)::int64 FROM udf(100000000, $1) group by grouping sets ((), (1)) order by 2 desc`

	rows, err := db.QueryContext(context.Background(), q, "123")
	check(err)
	defer rows.Close()

	columns, err := rows.Columns()
	check(err)

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		check(err)
		for i, value := range values {
			switch value.(type) {
			case nil:
				fmt.Print(columns[i], ": NULL")
			case []byte:
				fmt.Print(columns[i], ": ", string(value.([]byte)))
			default:
				fmt.Print(columns[i], ": ", value)
			}
			fmt.Printf("\nType: %s\n", reflect.TypeOf(value))
		}
		fmt.Println("-----------------------------------")
	}
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}
