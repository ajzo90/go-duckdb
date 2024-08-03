package aggregates

import (
	"github.com/marcboeker/go-duckdb"
	"github.com/matryer/is"
	"testing"
)

func TestAggregateWeightedSum(t *testing.T) {

	db, close := duckdb.AggregateTestConn[WeightedSumState]("weighted_sum", WeightedSumAggregate{})
	defer close()

	is := is.New(t)

	var res uint64
	is.NoErr(db.QueryRow("select weighted_sum(distinct 40,2)").Scan(&res))
	is.Equal(uint64(80), res)

	is.NoErr(db.QueryRow("SELECT weighted_sum(distinct i, 2) FROM range(100) t(i)").Scan(&res))
	is.Equal(uint64(9900), res)

	//var b bool
	//is.NoErr(db.QueryRow("SELECT weighted_sum(NULL, 2) is null").Scan(&b))
	//is.Equal(true, b)

	is.NoErr(db.QueryRow("SELECT weighted_sum(i, 2) FROM range(100*1000*1000) t(i)").Scan(&res))
	is.Equal(uint64(9999999900000000), res)

}
