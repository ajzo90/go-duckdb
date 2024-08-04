package functions

import (
	"github.com/marcboeker/go-duckdb"
	"github.com/matryer/is"
	"testing"
)

func TestNormalize(t *testing.T) {
	db, close := duckdb.ScalarTestConn("normalize32float", Normalize{})
	defer close()

	var res float32
	is.New(t).NoErr(db.QueryRow("SELECT normalize32float([1,1,1]::float[3])").Scan(&res))
	is.New(t).Equal(res, float32(1.7320508))

}
