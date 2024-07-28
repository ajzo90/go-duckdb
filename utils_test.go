package duckdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseEnum(t *testing.T) {
	testEnum(t, "ENUM('a', 'b',   'c')", []string{"a", "b", "c"})
	testEnum(t, "ENUM('a', 'b , 1',   'c')", []string{"a", "b , 1", "c"})
	testEnum(t, "ENUM(  'a''', 'b , 1',   'c')", []string{"a'", "b , 1", "c"})
}

func testEnum(t *testing.T, in string, exp []string) {
	res, isEnum, err := ParseEnum(in)
	require.NoError(t, err)
	require.True(t, isEnum)
	require.Equal(t, res, exp)
}
