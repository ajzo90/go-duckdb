package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"io"
)

func main() {
	connector := Must(duckdb.NewConnector("", nil))
	defer connector.Close()

	conn := Must(connector.Connect(context.Background()))
	defer conn.Close()

	q := `
WITH cities(Name, Id) AS (VALUES ('Amsterdam', 1), ('London', 2))
SELECT * FROM cities`

	stmt := Must(conn.Prepare(q))
	defer stmt.Close()

	rows := Must(stmt.(driver.StmtQueryContext).QueryContext(context.Background(), []driver.NamedValue{})).(duckdb.VecScanner)
	defer rows.Close()

	var ch duckdb.Chunk
	defer ch.Close()

	for {
		if err := rows.NextChunk(&ch); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		names := Must(ch.String(0))
		ids := Must(ch.Int32(1))

		for _, name := range names {
			fmt.Println(string(name.String()))
		}
		fmt.Println(ids)
	}

}

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
