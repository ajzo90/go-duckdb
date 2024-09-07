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

func (m ArraySumAggregateFunc) Update(aggs []*ArraySumAggregateState, ctx *duckdb.ExecContext) {
	x := duckdb.ArrayType[float32]{}
	_ = x.LoadCtx(ctx, 0, ctx.ChunkSize())

	for i := range len(aggs) {
		var state = aggs[i][:]
		row := x.GetRow(i)[:len(state)]
		for len(row) >= 4 && len(state) >= 4 {
			state[0] += row[0]
			state[1] += row[1]
			state[2] += row[2]
			state[3] += row[3]
			row, state = row[4:], state[4:]
		}
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

func (m ArraySumAggregateFunc) Finalize(states []*ArraySumAggregateState, ctx *duckdb.ExecContext) {
	x := duckdb.ArrayType[float32]{}
	var sz = ctx.ChunkSize()
	_ = x.LoadVecCtx(ctx, sz)
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
