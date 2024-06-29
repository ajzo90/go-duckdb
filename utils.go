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

func parseEnum(s string) ([]string, bool, error) {
	if after, ok := strings.CutPrefix(s, "ENUM("); ok {
		if enum, ok := strings.CutSuffix(after, ")"); ok {
			var values []string
			parts := strings.Split(enum, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if len(p) >= 2 && p[0] == '\'' && p[len(p)-1] == '\'' {
					values = append(values, p[1:len(p)-1])
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

	// VARCHAR[2]
	//if m := arrRegexp.FindStringSubmatch(sqlType); len(m) > 0 && m[0] == sqlType {
	//	sz, err := strconv.Atoi(m[1])
	//	if before, ok := strings.CutSuffix(sqlType, "["+m[1]+"]"); ok && err == nil {
	//		logicalTypeBase, err := createLogicalFromSQLType(before)
	//		if err != nil {
	//			return nil, err
	//		}
	//		logicalType := C.duckdb_create_array_type(logicalTypeBase, C.ulong(sz))
	//		C.duckdb_destroy_logical_type(&logicalTypeBase)
	//		return logicalType, nil
	//	}
	//}

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

	// ENUM('a', 'b')
	if values, isEnum, err := parseEnum(sqlType); isEnum {
		if err != nil {
			return nil, err
		}
		return createEnum(values), nil
	}

	// VARCHAR[]
	if before, ok := strings.CutSuffix(sqlType, "[]"); ok {
		logicalTypeBase, err := createLogicalFromSQLType(before)
		if err != nil {
			return nil, err
		}
		logicalType := C.duckdb_create_list_type(logicalTypeBase)
		C.duckdb_destroy_logical_type(&logicalTypeBase)
		return logicalType, nil
	}

	// primitive types
	if duckdbType, ok := SQLToDuckDBMap[sqlType]; ok {
		logicalType := C.duckdb_create_logical_type(duckdbType)
		return logicalType, nil
	} else {
		return nil, unsupportedTypeError(sqlType)
	}
}

func createLogicalFromGoValue(v any) (C.duckdb_logical_type, error) {
	if _, ok := v.([]string); ok {
		typ, err := getDuckdbTypeFromValue("")
		if err != nil {
			return nil, err
		}
		listTyp := C.duckdb_create_list_type(C.duckdb_create_logical_type(typ))
		return listTyp, nil
	} else if _, ok := v.([]uint32); ok {
		typ, err := getDuckdbTypeFromValue(uint32(0))
		if err != nil {
			return nil, err
		}
		listTyp := C.duckdb_create_list_type(C.duckdb_create_logical_type(typ))
		return listTyp, err
	} else if enum, ok := v.(*Enum); ok {
		typ := createEnum(enum.Names())
		return typ, nil
	} else {
		typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			return nil, err
		}
		return C.duckdb_create_logical_type(typ), nil
	}
}

// end with [\d*]
var arrRegexp = regexp.MustCompile(`^.*\[(\d+)]$`)

var decimalRegexp = regexp.MustCompile(`^DECIMAL\((\d+),(\d+)\)$`)

var mapReqExp = regexp.MustCompile(`^MAP\((\w),(\w)\)$`)
