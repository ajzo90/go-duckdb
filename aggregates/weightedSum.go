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

func (m WeightedSumAggregate) Update(aggs []*WeightedSumState, ch *duckdb.UDFDataChunk) {
	inputData, _ := duckdb.GetVector[int64](ch, 0)
	weightData, _ := duckdb.GetVector[int64](ch, 1)

	for i := range aggs {
		aggs[i].Sum += inputData[i] * weightData[i]
	}
}

func (m WeightedSumAggregate) Combine(s, t []*WeightedSumState) {
	for i := range s {
		t[i].Sum += s[i].Sum
	}
}

func (m WeightedSumAggregate) Finalize(states []*WeightedSumState, out *duckdb.Vector) {
	vv := duckdb.VectorData[int64](out)
	for i := range states {
		vv[i] = states[i].Sum
	}
}
