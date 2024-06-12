package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"log"
	"reflect"
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
}

func newMyTableUDF() *myTableUDF {
	return &myTableUDF{}
}

func (d *myTableUDF) NamedArguments() map[string]any {
	return map[string]any{
		"str": "",
	}
}

func (d *myTableUDF) Arguments() []any {
	return []any{
		int64(0),
		"",
		true,
	}
}

func (d *myTableUDF) Bind(namedArgs map[string]any, args []any) (b duckdb.Binding, err error) {
	var rows = args[0].(int64)
	var strVal = args[1].(string)
	var bVal = args[2].(bool)

	for k, v := range namedArgs {
		fmt.Println(k, v)
	}

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

	return schema, nil
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

func (schema *schema) Table() *duckdb.Table {
	return schema.schema
}

func (schema *schema) InitScanner(vecSize int, projection []int) (scanner duckdb.Scanner) {

	s := &udfScanner{
		schema:  schema,
		vecSize: vecSize,
		fns:     make([]func(vector *duckdb.Vector), 0, len(projection)),
	}

	for _, pos := range projection {
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

	return s
}

func main() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())

	check(duckdb.RegisterTableUDF(conn, "range2", duckdb.UDFOptions{ProjectionPushdown: true}, newMyTableUDF()))
	check(db.Ping())
	t0 := time.Now()
	defer func() {
		fmt.Println(time.Since(t0))
	}()
	const q = `SELECT xxx, len(xxx), count(userId) FROM range2(100000000, $str, $bbb) where userId=$user group by xxx`

	rows, err := db.QueryContext(context.Background(), q, sql.Named("str", ""), sql.Named("bbb", true), sql.Named("user", 1))
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

func check(args ...any) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}
