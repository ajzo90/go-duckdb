package duckdb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

type scalarUDF struct {
	err error
}

func (udf *scalarUDF) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{"INT", "INT"},
		ResultType: "INT",
	}
}

func (udf *scalarUDF) Exec(in *UDFDataChunk, out *UDFDataChunk) error {

	a := in.Columns[0].uint32s
	b := in.Columns[1].uint32s
	o := out.Columns[0].uint32s

	for i := range a {
		o[i] = a[i] + b[i]
	}

	return nil
}

func (udf *scalarUDF) SetError(err error) {
	udf.err = err
}

func TestScalarUDFPrimitive(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	var udf scalarUDF
	err = RegisterScalarUDF(c, "my_sum", &udf)

	var msg int
	row := db.QueryRow(`SELECT SUM(my_sum(10, 42::uint8)) AS msg from range(1000)`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 52*1000, msg)
	require.NoError(t, db.Close())

	// TODO: test other primitive data types
}

func TestScalarUDFErrors(t *testing.T) {
	// TODO: trigger all possible errors and move to errors_test.go
}

func TestScalarUDFNested(t *testing.T) {
	// TODO: test nested data types
}
