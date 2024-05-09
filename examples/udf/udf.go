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
	"time"
)

type schema struct {
	schema *duckdb.Table
	rows   int64
	strVal string
	bVal   bool
	fVal   float64
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
		true,
	}
}

func (d *myTableUDF) BindArguments(args ...any) (schem duckdb.Ref, err error) {
	var rows = args[0].(int64)
	var strVal = args[1].(string)
	var bVal = args[2].(bool)
	schema := &schema{
		rows:   rows,
		strVal: strVal,
		bVal:   bVal,
		schema: &duckdb.Table{
			Columns: []duckdb.ColumnDef{
				{"userId", int64(0)},
				{"xxx", strVal},
				{"b", bVal},
			},
			Cardinality: int(rows),
			MaxThreads:  12,
		},
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()

	ref := duckdb.Ref(len(d.schemas))
	d.schemas[ref] = schema
	return ref, nil
}

func (d *myTableUDF) DestroyTable(ref duckdb.Ref) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	delete(d.schemas, ref)
}

func (d *myTableUDF) GetTable(ref duckdb.Ref) *duckdb.Table {
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

func (l *udfScanner) Close() {
	log.Println("close scanner")
}

func (l *udfScanner) Scan(ch *duckdb.DataChunk) (int, error) {
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

	return l.vecSize, nil
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
					vec.AppendUInt64(uint64(i % 10))
				}
			}
		case 1:
			fn = func(vec *duckdb.Vector) {
				for i := 0; i < s.vecSize; i++ {
					s.scr = append(s.scr[:0], schema.strVal...)
					vec.AppendBytes(s.scr)
				}
			}
		case 2:
			fn = func(vec *duckdb.Vector) {
				for i := 0; i < s.vecSize; i++ {
					if i%2 == 0 {
						vec.AppendBool(schema.bVal)
					} else {
						vec.AppendNull()
					}
				}

			}
		case 3:
			fn = func(vec *duckdb.Vector) {
				for i := 0; i < s.vecSize; i++ {
					if i%2 == 0 {
						vec.AppendFloat64(schema.fVal)
					} else {
						vec.AppendNull()
					}
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

	check(duckdb.RegisterTableUDF(conn, "siftlab", duckdb.UDFOptions{ProjectionPushdown: true}, newMyTableUDF()))
	check(db.Ping())
	t0 := time.Now()
	defer func() {
		fmt.Println(time.Since(t0))
	}()
	const q = `SELECT count(userId) FROM siftlab(10000000000, $str, $bbb)`

	rows, err := db.QueryContext(context.Background(), q, sql.Named("str", "123"), sql.Named("bbb", true))
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
