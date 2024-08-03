package aggregates

import (
	"github.com/marcboeker/go-duckdb"
)

// ARRAY SUM DEFINITION
type ArraySumAggregateFunc struct {
}

type ArraySumAggregateState [4]float32

func (m ArraySumAggregateFunc) Config() duckdb.AggregateFunctionConfig {
	return duckdb.AggregateFunctionConfig{
		InputTypes: []string{"FLOAT[4]"},
		ResultType: "FLOAT[4]",
	}
}

func (m ArraySumAggregateFunc) Init(stateType *ArraySumAggregateState) {
	clear(stateType[:])
}

func (m ArraySumAggregateFunc) Update(aggs []*ArraySumAggregateState, chunk *duckdb.UDFDataChunk) {
	x := duckdb.ArrayType[float32]{}
	_ = x.Load(chunk, 0)

	for i := range aggs {
		row := x.GetRow(i)
		for j := range row {
			aggs[i][j] += row[j]
		}
	}
}

func (m ArraySumAggregateFunc) Combine(source, target []*ArraySumAggregateState) {
	for i := range source {
		for j := range source[i] {
			target[i][j] += source[i][j]
		}
	}
}

func (m ArraySumAggregateFunc) Finalize(states []*ArraySumAggregateState, out *duckdb.Vector) {
	x := duckdb.ArrayType[float32]{}
	_ = x.LoadVec(out, len(states))
	for i := range states {
		row := x.GetRow(i)
		for j := range row {
			row[j] = states[i][j]
		}
	}
}

func (m ArraySumAggregateFunc) Destroy(aggs []*ArraySumAggregateState) {
}
