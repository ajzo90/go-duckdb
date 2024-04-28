package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"log"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
)

type schema struct {
	schema *duckdb.Schema
	rows   int64
}

type myTableUDF struct {
	schemas       map[duckdb.Ref]*schema
	localStateMtx sync.RWMutex
	localStates   map[duckdb.Ref]*localState
}

func (d *myTableUDF) GetArguments() []any {
	return []any{
		int64(0),
		any(""),
	}
}

func (d *myTableUDF) BindArguments(args ...any) (schem duckdb.Ref) {
	ref := duckdb.Ref(len(d.schemas))
	var rows = args[0].(int64)
	d.schemas[ref] = &schema{
		rows: rows,
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
	return ref
}

func (d *myTableUDF) GetSchema(ref duckdb.Ref) *duckdb.Schema {
	return d.schemas[ref].schema
}

type localState struct {
	schema  *schema
	vecSize int
	scr     []byte
	fns     []func(vector *duckdb.Vector)
}

func (l *localState) Scan(ch *duckdb.DataChunk) int {
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
	d.localStateMtx.RLock()
	defer d.localStateMtx.RUnlock()

	return d.localStates[ref]
}

func (d *myTableUDF) InitScanner(parent duckdb.Ref, vecSize int) (scanner duckdb.Ref) {

	schema := d.schemas[parent]
	s := &localState{schema: schema, vecSize: vecSize, fns: make([]func(vector *duckdb.Vector), 0, len(schema.schema.Projection))}
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
					vec.AppendBytes(strconv.AppendInt(s.scr[:0], int64(i%10), 10))
				}
			}
		}
		s.fns = append(s.fns, fn)
	}

	d.localStateMtx.Lock()
	defer d.localStateMtx.Unlock()

	ref := duckdb.Ref(len(d.localStates))
	d.localStates[ref] = s
	return ref
}

func newMyTableUDF() *myTableUDF {
	return &myTableUDF{
		schemas:     map[duckdb.Ref]*schema{},
		localStates: map[duckdb.Ref]*localState{},
	}
}

func main() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())

	if err := duckdb.RegisterTableUDF(conn, "udf", duckdb.UDFOptions{ProjectionPushdown: true}, newMyTableUDF()); err != nil {
		panic(err)
	}
	check(db.Ping())

	rows, err := db.QueryContext(context.Background(), " SELECT result, count(*)::int64 FROM udf(100000000, '123') group by grouping sets ((), (1)) order by 2 desc")
	check(err)
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
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
