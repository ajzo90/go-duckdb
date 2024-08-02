package duckdb

import (
	"context"
	"database/sql"
	"github.com/matryer/is"
	"testing"
)

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestRegisterAggregate(t *testing.T) {

	connector := Must(NewConnector("?max_memory=100M", nil))
	defer connector.Close()

	conn := Must(connector.Connect(context.Background()))

	is := is.New(t)

	is.NoErr(RegisterAggregateUDFConn[MyWeightedSumState, int64](conn, "my_weighted_sum", MyWeightedSumAggregate{}))

	db := sql.OpenDB(connector)

	var res uint64
	is.NoErr(db.QueryRow("select my_weighted_sum(40,2)").Scan(&res))
	is.Equal(uint64(80), res)

	is.NoErr(db.QueryRow("SELECT my_weighted_sum(i, 2) FROM range(100) t(i)").Scan(&res))
	is.Equal(uint64(9900), res)

}

type MyWeightedSumState struct {
	Sum int64
}

type MyWeightedSumAggregate struct {
}

func (m MyWeightedSumAggregate) Init(state *MyWeightedSumState) {
	*state = MyWeightedSumState{Sum: 0}
}

func (m MyWeightedSumAggregate) Update(aggs []*MyWeightedSumState, ch *UDFDataChunk) {
	inputData, _ := GetVector[int64](ch, 0)
	weightData, _ := GetVector[int64](ch, 1)

	for i := range aggs {
		aggs[i].Sum += inputData[i] * weightData[i]
	}
}

func (m MyWeightedSumAggregate) Combine(s, t []*MyWeightedSumState) {
	for i := range s {
		t[i].Sum += s[i].Sum
	}
}

func (m MyWeightedSumAggregate) Config() AggregateFunctionConfig {
	return AggregateFunctionConfig{
		InputTypes: []string{"BIGINT", "BIGINT"},
		ResultType: "BIGINT",
	}
}

func (m MyWeightedSumAggregate) Finalize(state *MyWeightedSumState) int64 {
	return state.Sum
}
