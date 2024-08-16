package aggregates

import (
	"fmt"
	"github.com/marcboeker/go-duckdb"
)

// ARRAY SUM DEFINITION
type ArraySumAggregateFunc struct {
}

const vecSize = 32

type ArraySumAggregateState [vecSize]float32

var vecSqlType = fmt.Sprintf("FLOAT[%d]", vecSize)

func (m ArraySumAggregateFunc) Config() duckdb.AggregateFunctionConfig {
	return duckdb.AggregateFunctionConfig{
		InputTypes: []string{vecSqlType},
		ResultType: vecSqlType,
	}
}

func (m ArraySumAggregateFunc) Init(stateType *ArraySumAggregateState) {
	clear(stateType[:])
}

func (m ArraySumAggregateFunc) Update(aggs []*ArraySumAggregateState, chunk *duckdb.UDFDataChunk) {
	x := duckdb.ArrayType[float32]{}
	_ = x.Load(chunk, 0)

	for i := range len(aggs) {
		var state = aggs[i]
		row := x.GetRow(i)[:len(state)]
		for i := range row {
			state[i] += row[i]
		}
	}

}

func (m ArraySumAggregateFunc) Combine(source, target []*ArraySumAggregateState) {
	for i := range source {
		var from = source[i]
		var to = target[i][:len(from)]
		for j := range from {
			to[j] += from[j]
		}
	}
}

func (m ArraySumAggregateFunc) Finalize(states []*ArraySumAggregateState, out *duckdb.Vector) {
	x := duckdb.ArrayType[float32]{}
	_ = x.LoadVec(out, len(states))
	for i := range states {
		var state = states[i]
		row := x.GetRow(i)[:len(state)]
		for j := range row {
			row[j] = state[j]
		}
	}
}

func (m ArraySumAggregateFunc) Destroy(aggs []*ArraySumAggregateState) {
}
