package duckdb

import (
	"context"
	"database/sql"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type MySum struct{}

func (udf MySum) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{INT, INT},
		ResultType: INT,
	}
}

func (udf MySum) Exec(in *UDFDataChunk, out *Vector) {
	a, _ := GetVector[int32](in, 0)
	b, _ := GetVector[int32](in, 1)

	for i := range a {
		Append(out, a[i]+b[i])
	}
}

type MyConcat struct {
}

func (udf MyConcat) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{VARCHAR, VARCHAR},
		ResultType: VARCHAR,
	}
}

func (udf MyConcat) Exec(in *UDFDataChunk, out *Vector) {
	a, _ := GetVector[Varchar](in, 0)
	b, _ := GetVector[Varchar](in, 1)

	var buf = make([]byte, 0, 4096)
	for i := range a {
		buf = buf[:0]
		buf = append(buf, a[i].Bytes()...)
		buf = append(buf, b[i].Bytes()...)
		AppendBytes(out, buf)
	}
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

}

func TestScalarUDFString(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = RegisterScalarUDF(c, "my_concat", MyConcat{})
	require.NoError(t, err)

	var msg string
	row := db.QueryRow(`SELECT my_concat('ac', 'dc') AS msg from range(10)`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, "acdc", msg)

}

type MyListShuffle struct{}

func (udf MyListShuffle) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{`VARCHAR[]`, `INT`},
		ResultType: `VARCHAR[]`,
	}
}

func (udf MyListShuffle) Exec(in *UDFDataChunk, out *Vector) {
	a, _ := List[Varchar](in, 0)
	b, _ := GetVector[int32](in, 1)
	num := int(b[0])

	out.ReserveListSize(a.Rows() * num)

	child := out.Child()

	var listSz int
	for i := 0; i < a.Rows(); i++ {
		row := a.GetRow(i)
		rand.Shuffle(len(a.elements), func(i, j int) {
			a.elements[i], a.elements[j] = a.elements[j], a.elements[i]
		})

		sz := min(num, len(row))
		out.AppendListEntry(sz)
		for _, v := range row[:sz] {
			AppendBytes(child, v.Bytes())
			listSz++
		}
	}

	out.SetListSize(listSz)
}

func TestScalarUDFListShuffle(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = RegisterScalarUDF(c, "my_shuffle", MyListShuffle{})
	require.NoError(t, err)

	var msg string
	row := db.QueryRow(`SELECT to_json(my_shuffle(['ac', 'dc'], 1)) AS msg from range(2048)`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, `["ac"]`, msg)

}
