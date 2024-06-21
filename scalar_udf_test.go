package duckdb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

type MySum struct{}

func (udf MySum) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{"INT", "INT"},
		ResultType: "INT",
	}
}

func (udf MySum) Exec(in *UDFDataChunk, out *UDFDataChunk) error {

	a := in.Columns[0].uint32s
	b := in.Columns[1].uint32s
	o := out.Columns[0].uint32s

	for i := range a {
		o[i] = a[i] + b[i]
	}

	return nil
}

func TestScalarUDFPrimitive(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = RegisterScalarUDF(c, "my_sum", MySum{})
	require.NoError(t, err)

	var msg int
	row := db.QueryRow(`SELECT SUM(my_sum(10, range::int)) AS msg from range(1000000)`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 500009500000, msg)
	require.NoError(t, db.Close())

}
