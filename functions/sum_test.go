package functions

import (
	"context"
	"database/sql"
	"github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSum(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = duckdb.RegisterScalarUDF(c, "my_sum", MySum{})
	require.NoError(t, err)

	var msg int
	row := db.QueryRow(`SELECT SUM(my_sum(10, range::int)) AS msg from range(1000000)`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 500009500000, msg)

}
