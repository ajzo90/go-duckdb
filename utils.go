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

func StringifyEnum(values []string) string {
	var b = make([]byte, 0, 4096)
	b = append(b, "ENUM("...)
	for i, v := range values {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '\'')
		b = append(b, strings.ReplaceAll(v, "'", "''")...)
		b = append(b, '\'')
	}
	b = append(b, ')')
	return string(b)
}

func parseEnum(in string) ([]string, bool, error) {
	var enum = in
	var arr []string
	for {
		enum = strings.TrimLeft(enum, " ")
		firstIdx := strings.IndexByte(enum, '\'')
		if firstIdx != 0 {
			return nil, true, fmt.Errorf("invalid %v", enum)
		}
		enum = enum[1:]
		init := enum

	next:
		nextIdx := strings.IndexByte(enum, '\'')
		if nextIdx == -1 {
			return nil, true, fmt.Errorf("expect ' here")
		} else if nextIdx+1 < len(enum) && enum[nextIdx+1] == '\'' {
			enum = enum[nextIdx+2:]
			goto next
		}
		enum = enum[nextIdx+1:]
		s := init[:len(init)-len(enum)-1]
		s = strings.ReplaceAll(s, "''", "'")
		for _, v := range arr {
			if v == s {
				return nil, true, fmt.Errorf("enum contains duplicate values '%s'", v)
			}
		}

		arr = append(arr, s)
		enum = strings.TrimLeft(enum, " ")
		if len(enum) == 0 {
			return arr, true, nil
		} else if enum[0] != ',' {
			return nil, true, fmt.Errorf("expect , here [%s]", enum)
		}
		enum = enum[1:]
	}
}

func ParseEnum(s string) ([]string, bool, error) {
	var prefix = "ENUM("
	if len(s) > len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) && s[len(s)-1] == ')' {
		return parseEnum(s[len(prefix) : len(s)-1])
	}
	return nil, false, nil
}

func createLogicalFromSQLType(origType string) (C.duckdb_logical_type, error) {

	sqlType := strings.ToUpper(strings.ReplaceAll(origType, " ", ""))

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
	if values, isEnum, err := ParseEnum(origType); isEnum {
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
