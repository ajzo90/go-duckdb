package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"math/rand"
	"testing"
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
	var a ListType[Varchar]
	_ = a.Load(in, 0)

	var b Vec[int32]
	_ = b.Load(in, 1)

	num := int(b.Data[0])
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
	row := db.QueryRow(`SELECT  to_json(my_shuffle(['ac', 'dc'], 1)) AS msg from range(2048)`)
	require.NoError(t, row.Scan(&msg))
	if msg == `["ac"]` || msg == `["dc"]` {
	} else {
		require.Equal(t, `["ac"]`, msg)
	}

}

type MyArrFn struct{}

func (udf MyArrFn) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{`INT[3]`},
		ResultType: `INT`,
	}
}

func (udf MyArrFn) Exec(in *UDFDataChunk, out *Vector) {
	var a ArrayType[int32]
	err := a.Load(in, 0)
	if err != nil {
		panic(err)
	}
	for i := 0; i < a.Rows(); i++ {
		els := a.GetRow(i)
		var v int32
		for _, x := range els {
			v += x
		}
		Append(out, v)
	}
}

func TestScalarUDFListShuffle2(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = RegisterScalarUDF(c, "my_fn", MyArrFn{})
	require.NoError(t, err)

	var msg int
	row := db.QueryRow(`SELECT sum(my_fn([1,1,range]::int[3])) from range(2048)`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 2048*(4+(2047))/2, msg)

}

type DecimalFn struct{}

func (udf DecimalFn) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{`DECIMAL(7,2)`},
		ResultType: `INT`,
	}
}

func (udf DecimalFn) Exec(in *UDFDataChunk, out *Vector) {
	var a DecimalType
	err := a.Load(in, 0)
	if err != nil {
		panic(err)
	}
	for i := 0; i < a.Rows(); i++ {
		x := a.GetRow(i)
		Append(out, int32(x.Float64()))
	}
}

func TestScalarUDFListShuffle23(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = RegisterScalarUDF(c, "my_fn", DecimalFn{})
	require.NoError(t, err)

	var msg int32
	row := db.QueryRow(`SELECT my_fn(4.2+range) from range(2048)`, sql.Named("str", `[A,B]`))
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, int32(4), msg)
}

func TestX(t *testing.T) {
	v := decimalRegexp.FindStringSubmatch(`DECIMAL(2,21)`)
	if v[1] == "2" && v[2] == "21" {
	} else {
		t.Fail()
	}
}

func TestXXXX(t *testing.T) {
	connector, err := NewConnector("", nil)
	require.NoError(t, err)

	conn, err := connector.ConnectRaw(context.Background())
	require.NoError(t, err)

	q := `
WITH x(a) AS(VALUES('foo'), ('bar'), ('foo'), ('foo')) 
FROM x SELECT $json::JSON xx, a::ENUM('foo', 'bar'), typeof(xx)`

	stmt, err := conn.PrepareContext(context.Background(), q)
	require.NoError(t, err)

	rows, err := stmt.QueryContextRaw(context.Background(), []driver.NamedValue{{Name: "json", Value: `{"x":1}`}})
	require.NoError(t, err)

	var ch Chunk
	var enum EnumType
	var str, str2 Vec[Varchar]
	for {
		if err := rows.NextChunk(&ch); err == io.EOF {
			break
		} else if err != nil {
			require.Fail(t, err.Error())
		}
		require.NoError(t, str.Load(&ch, 0))
		require.NoError(t, enum.Load(&ch, 1))
		require.NoError(t, str2.Load(&ch, 2))
		for i := 0; i < enum.Rows(); i++ {
			log.Println(string(enum.GetBytes(i)))
		}
		for i := 0; i < len(str.Data); i++ {
			log.Println(string(str.Data[i].Bytes()))
		}
		for i := 0; i < len(str2.Data); i++ {
			log.Println(string(str2.Data[i].Bytes()))
		}

	}

}
