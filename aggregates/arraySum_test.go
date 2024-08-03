package aggregates

import (
	"github.com/marcboeker/go-duckdb"
	"github.com/matryer/is"
	"testing"
)

func TestAggregateArraySum(t *testing.T) {

	db, close := duckdb.AggregateTestConn[ArraySumAggregateState]("array_sum", ArraySumAggregateFunc{})
	defer close()

	is := is.New(t)

	var f0, f1, f2, f3 float32
	is.NoErr(db.QueryRow("SELECT a[1], a[2], a[3], a[4] from (select array_sum([1,2,3,i]::float[4]) AS a FROM range(100) t(i))").Scan(&f0, &f1, &f2, &f3))
	is.Equal(float32(100), f0)
	is.Equal(float32(200), f1)
	is.Equal(float32(300), f2)
	is.Equal(float32(4950), f3)
}
