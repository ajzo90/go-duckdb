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

	conn := Must(connector.ConnectRaw(context.Background()))
	defer conn.Close()

	q := `
WITH cities(Name, Id) AS (VALUES ('Amsterdam', 1), ('London', 2))
SELECT *, version() FROM cities`

	stmt := Must(conn.PrepareContext(context.Background(), q))
	defer stmt.Close()

	rows := Must(stmt.QueryContextRaw(context.Background(), []driver.NamedValue{}))
	defer rows.Close()

	var ch duckdb.Chunk
	defer ch.Close()

	for {
		if err := rows.NextChunk(&ch); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		for _, v := range Must(ch.String(0)) {
			fmt.Println(string(v.String()))
		}
		fmt.Println(Must(ch.Int32(1)))
		for _, v := range Must(ch.String(2)) {
			fmt.Println(string(v.String()))
		}

	}

}

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
