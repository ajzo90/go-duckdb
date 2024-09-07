package aggregates

import "github.com/marcboeker/go-duckdb"

// WEIGHTED SUM DEFINITION

type WeightedSumState struct {
	Sum int64
}

type WeightedSumAggregate struct {
}

func (m WeightedSumAggregate) Config() duckdb.AggregateFunctionConfig {
	return duckdb.AggregateFunctionConfig{
		InputTypes: []string{duckdb.BIGINT, duckdb.BIGINT},
		ResultType: duckdb.BIGINT,
	}
}

func (m WeightedSumAggregate) Init(state *WeightedSumState) {
	*state = WeightedSumState{Sum: 0}
}

func (m WeightedSumAggregate) Destroy(aggs []*WeightedSumState) {
}

func (m WeightedSumAggregate) Update(aggs []*WeightedSumState, ch *duckdb.ExecContext) {
	sz := ch.ChunkSize()
	x := duckdb.Vec[int64]{}
	_ = x.LoadCtx(ch, 0, sz)

	w := duckdb.Vec[int64]{}
	_ = w.LoadCtx(ch, 1, sz)

	inputData := x.Data
	weightData := w.Data

	for i := range aggs {
		aggs[i].Sum += inputData[i] * weightData[i]
	}
}

func (m WeightedSumAggregate) Combine(s, t []*WeightedSumState) {
	for i := range s {
		t[i].Sum += s[i].Sum
	}
}

func (m WeightedSumAggregate) Finalize(states []*WeightedSumState, ctx *duckdb.ExecContext) {
	vv := duckdb.UDFScalarVectorResult[int64](ctx)
	for i := range states {
		vv[i] = states[i].Sum
	}
}
