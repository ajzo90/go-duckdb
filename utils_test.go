package duckdb

import (
	"github.com/matryer/is"
	"testing"
)

func TestParseEnum(t *testing.T) {
	testEnum(t, "ENUM('a', 'b',   'c')", []string{"a", "b", "c"})
	testEnum(t, "ENUM('a', 'b , 1',   'c')", []string{"a", "b , 1", "c"})
	testEnum(t, "ENUM(  'a''', 'b , 1',   'c')", []string{"a'", "b , 1", "c"})
}

func testEnum(t *testing.T, in string, exp []string) {
	res, isEnum, err := ParseEnum(in)
	is := is.New(t)
	is.NoErr(err)
	is.True(isEnum)
	is.Equal(res, exp)

}
