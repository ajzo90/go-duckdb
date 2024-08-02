package duckdb

import (
	"context"
	"database/sql"
	"github.com/matryer/is"
	"testing"
)

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestRegisterAggregate(t *testing.T) {

	connector := Must(NewConnector("?max_memory=100M", nil))
	defer connector.Close()

	conn := Must(connector.Connect(context.Background()))

	is := is.New(t)

	is.NoErr(RegisterAggregateUDFConn(conn, "my_weighted_sum", 1))

	db := sql.OpenDB(connector)

	var res uint64
	is.NoErr(db.QueryRow("select my_weighted_sum(40,2)").Scan(&res))
	is.Equal(uint64(80), res)

	is.NoErr(db.QueryRow("SELECT my_weighted_sum(i, 2) FROM range(100) t(i)").Scan(&res))
	is.Equal(uint64(9900), res)

}
