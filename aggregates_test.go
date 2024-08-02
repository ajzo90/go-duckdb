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

	is.NoErr(RegisterAggregateUDFConn[MyWeightedSumState](conn, "my_weighted_sum", MyWeightedSumAggregate{}))
	is.NoErr(RegisterAggregateUDFConn[MyArraySumState](conn, "array32_sum", MyArraySum{}))

	db := sql.OpenDB(connector)

	var res uint64
	is.NoErr(db.QueryRow("select my_weighted_sum(40,2)").Scan(&res))
	is.Equal(uint64(80), res)

	is.NoErr(db.QueryRow("SELECT my_weighted_sum(i, 2) FROM range(100) t(i)").Scan(&res))
	is.Equal(uint64(9900), res)

	//var b bool
	//is.NoErr(db.QueryRow("SELECT my_weighted_sum(NULL, 2) is null").Scan(&b))
	//is.Equal(true, b)

	is.NoErr(db.QueryRow("SELECT my_weighted_sum(i, 2) FROM range(100*1000*1000) t(i)").Scan(&res))
	is.Equal(uint64(9999999900000000), res)

	var f0, f1, f2, f3 float32
	is.NoErr(db.QueryRow("SELECT a[1], a[2], a[3], a[4] from (select array32_sum([1,2,3,i]::float[4]) AS a FROM range(100) t(i))").Scan(&f0, &f1, &f2, &f3))
	is.Equal(float32(100), f0)
	is.Equal(float32(200), f1)
	is.Equal(float32(300), f2)
	is.Equal(float32(4950), f3)

}

// WEIGHTED SUM DEFINITION

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

func (m MyWeightedSumAggregate) Finalize(states []*MyWeightedSumState, out *Vector) {
	vv := vectorData[int64](out)
	for i := range states {
		vv[i] = states[i].Sum
	}
}

// ARRAY SUM DEFINITION
type MyArraySum struct {
}

type MyArraySumState [4]float32

func (m MyArraySum) Config() AggregateFunctionConfig {
	return AggregateFunctionConfig{
		InputTypes: []string{"FLOAT[4]"},
		ResultType: "FLOAT[4]",
	}
}

func (m MyArraySum) Init(stateType *MyArraySumState) {
	clear(stateType[:])
}

func (m MyArraySum) Update(aggs []*MyArraySumState, chunk *UDFDataChunk) {
	x := ArrayType[float32]{}
	_ = x.Load(chunk, 0)

	for i := range aggs {
		row := x.GetRow(i)
		for j := range row {
			aggs[i][j] += row[j]
		}
	}
}

func (m MyArraySum) Combine(source, target []*MyArraySumState) {
	for i := range source {
		for j := range source[i] {
			target[i][j] += source[i][j]
		}
	}
}

func (m MyArraySum) Finalize(states []*MyArraySumState, out *Vector) {
	x := ArrayType[float32]{}
	_ = x.LoadVec(out, len(states))
	for i := range states {
		row := x.GetRow(i)
		for j := range row {
			row[j] = states[i][j]
		}
	}
}
