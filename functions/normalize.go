package functions

import (
	"github.com/marcboeker/go-duckdb"
	"math"
)

type Normalize struct {
}

func (udf Normalize) Config() duckdb.ScalarFunctionConfig {
	return duckdb.ScalarFunctionConfig{
		InputTypes: []string{"FLOAT[3]"},
		ResultType: "FLOAT",
	}
}

func (udf Normalize) Exec(in *duckdb.UDFDataChunk, out *duckdb.Vector) error {
	var a duckdb.ArrayType[float32]
	_ = a.Load(in, 0)

	for i := 0; i < a.Rows(); i++ {
		var norm float32
		for _, v := range a.GetRow(i) {
			norm += v * v
		}
		duckdb.Append(out, float32(math.Sqrt(float64(norm))))
	}
	return nil
}
