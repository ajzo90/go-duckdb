package duckdb

import (
	"database/sql/driver"
	"github.com/matryer/is"
	"testing"
)

type VarcharToTinyInt struct {
}

func (v VarcharToTinyInt) Config() CastFunctionConfig {
	return CastFunctionConfig{
		Source: "VARCHAR",
		Target: "TINYINT",
	}
}

func (v VarcharToTinyInt) Exec(ctx *CastExecContext) error {
	in := GetData[Varchar](ctx.Input())
	out := GetData[int8](ctx.Output())

	for i := range ctx.Count() {
		out[i] = int8(len(in[0].Bytes()))
	}
	return nil
}

func TestCast(t *testing.T) {

	db, close := TestConn(func(conn driver.Conn) {
		err := RegisterCast(conn, VarcharToTinyInt{})
		if err != nil {
			panic(err)
		}
	})
	defer close()
	is := is.New(t)

	var f0 float32
	is.NoErr(db.QueryRow("SELECT 'HELLO'::int8 from range(10)").Scan(&f0))
	is.Equal(float32(5), f0)

}
