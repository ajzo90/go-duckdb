package functions

import "github.com/marcboeker/go-duckdb"

type MySum struct{}

func (udf MySum) Config() duckdb.ScalarFunctionConfig {
	return duckdb.ScalarFunctionConfig{
		InputTypes: []string{duckdb.INT, duckdb.INT},
		ResultType: duckdb.INT,
	}
}

func (udf MySum) Exec(in *duckdb.UDFDataChunk, out *duckdb.Vector) error {
	a, _ := duckdb.GetVector[int32](in, 0)
	b, _ := duckdb.GetVector[int32](in, 1)

	for i := range a {
		duckdb.Append(out, a[i]+b[i])
	}
	return nil
}
