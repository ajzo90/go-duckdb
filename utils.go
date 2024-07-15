package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

*/
import "C"
import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func MapFn[From any, FromColl ~[]From, To any](from FromColl, f func(From) To) []To {
	var out = make([]To, len(from))
	for i, v := range from {
		out[i] = f(v)
	}
	return out
}

func StringifyEnum(values []string) string {
	return "ENUM(" + strings.Join(MapFn(values, func(s string) string {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}), ", ") + ")"
}

func ParseEnum(s string) ([]string, bool, error) {
	if after, ok := strings.CutPrefix(s, "ENUM("); ok {
		if enum, ok := strings.CutSuffix(after, ")"); ok {
			var values []string
			parts := strings.Split(enum, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if len(p) >= 2 && p[0] == '\'' && p[len(p)-1] == '\'' {
					x := p[1 : len(p)-1]
					x = strings.ReplaceAll(x, "''", "'")
					values = append(values, x)
				} else {
					return nil, true, fmt.Errorf("invalid ENUM value: %s", p)
				}
			}
			return values, true, nil
		}
	}
	return nil, false, nil
}

func createLogicalFromSQLType(sqlType string) (C.duckdb_logical_type, error) {

	sqlType = strings.ToUpper(strings.ReplaceAll(sqlType, " ", ""))

	// STRUCT(v VARCHAR, i INTEGER)
	if strings.HasPrefix(sqlType, "STRUCT(") {
		return nil, fmt.Errorf("struc is not supported")
	}

	// <TYPE>[2]
	if m := arrRegexp.FindStringSubmatch(sqlType); len(m) > 0 && m[0] == sqlType {
		sz, err := strconv.Atoi(m[1])
		if before, ok := strings.CutSuffix(sqlType, "["+m[1]+"]"); ok && err == nil {
			logicalTypeBase, err := createLogicalFromSQLType(before)
			if err != nil {
				return nil, err
			}
			logicalType := C.duckdb_create_array_type(logicalTypeBase, C.idx_t(sz))
			C.duckdb_destroy_logical_type(&logicalTypeBase)
			return logicalType, nil
		}
	}

	// <TYPE>[]
	if before, ok := strings.CutSuffix(sqlType, "[]"); ok {
		logicalTypeBase, err := createLogicalFromSQLType(before)
		if err != nil {
			return nil, err
		}
		logicalType := C.duckdb_create_list_type(logicalTypeBase)
		C.duckdb_destroy_logical_type(&logicalTypeBase)
		return logicalType, nil
	}

	// DECIMAL(3,2)
	if m := decimalRegexp.FindStringSubmatch(sqlType); len(m) == 3 && m[0] == sqlType {
		width, _ := strconv.Atoi(m[1])
		scale, _ := strconv.Atoi(m[2])
		logical := C.duckdb_create_decimal_type(C.uchar(width), C.uchar(scale))
		return logical, nil
	}

	// MAP(INTEGER,DOUBLE)
	if m := mapReqExp.FindStringSubmatch(sqlType); len(m) == 3 && m[0] == sqlType {
		keyType, err := createLogicalFromSQLType(m[1])
		if err != nil {
			return nil, err
		}
		defer C.duckdb_destroy_logical_type(&keyType)
		valType, err := createLogicalFromSQLType(m[2])
		if err != nil {
			return nil, err
		}
		defer C.duckdb_destroy_logical_type(&valType)
		logical := C.duckdb_create_map_type(keyType, valType)
		return logical, nil
	}

	// "anonymous" enum
	if sqlType == "ENUM" {
		return createEnum(nil), nil
	}

	// ENUM('a', 'b')
	if values, isEnum, err := ParseEnum(sqlType); isEnum {
		if err != nil {
			return nil, err
		}
		return createEnum(values), nil
	}

	// primitive types
	if duckdbType, ok := SQLToDuckDBMap[sqlType]; ok {
		return C.duckdb_create_logical_type(duckdbType), nil
	} else {
		return nil, unsupportedTypeError(sqlType)
	}
}

// end with [\d*]
var arrRegexp = regexp.MustCompile(`^.*\[(\d+)]$`)

var decimalRegexp = regexp.MustCompile(`^DECIMAL\((\d+),(\d+)\)$`)

var mapReqExp = regexp.MustCompile(`^MAP\((\w),(\w)\)$`)
