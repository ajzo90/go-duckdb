package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

*/
import "C"

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

var sqlToLogical = func() func(sql string) (C.duckdb_logical_type, error) {

	var db C.duckdb_database
	var con C.duckdb_connection

	if C.duckdb_open(nil, &db) == C.DuckDBError {
		panic(1)
	}
	//defer C.duckdb_close(&db)
	if C.duckdb_connect(db, &con) == C.DuckDBError {
		panic(1)
	}
	var dbMtx sync.Mutex

	//defer C.duckdb_disconnect(&con)

	var enumCache = map[string][]unsafe.Pointer{}
	var enumCacheMtx sync.Mutex

	var f func(sql string) (C.duckdb_logical_type, error)

	f = func(sql string) (C.duckdb_logical_type, error) {

		t, ok := SQLToDuckDBMap[strings.ToUpper(sql)]
		if ok {
			return C.duckdb_create_logical_type(t), nil
		}

		// list and array of primitive types
		if before, ok := strings.CutSuffix(sql, "]"); ok {
			var start = strings.IndexByte(sql, '[')
			var typ = before[:start]
			lt, err := f(typ)
			if err == nil {
				defer C.duckdb_destroy_logical_type(&lt)
				var size = before[start+1:]
				if len(size) == 0 {
					return C.duckdb_create_list_type(lt), nil
				} else if sz, err := strconv.Atoi(size); err == nil {
					return C.duckdb_create_array_type(lt, C.idx_t(sz)), nil
				}
			}
		}

	checkCache:

		enumCacheMtx.Lock()
		enumVals, ok := enumCache[sql]
		enumCacheMtx.Unlock()

		if ok {
			strs := (**C.char)(malloc(enumVals...))
			typ := C.duckdb_create_enum_type(strs, C.idx_t(len(enumVals)))
			C.free(unsafe.Pointer(strs))
			return typ, nil
		}

		if !dbMtx.TryLock() {
			goto checkCache
		}
		defer dbMtx.Unlock()

		q := fmt.Sprintf("SELECT CAST(NULL AS %s)", sql)
		//fmt.Println("create type from sql fallback", q)

		var result C.duckdb_result

		qStr := C.CString(q)
		defer C.free(unsafe.Pointer(qStr))

		defer C.duckdb_destroy_result(&result)

		state := C.duckdb_query(con, qStr, &result)
		if state == C.DuckDBError {
			return nil, fmt.Errorf("failed to execute query")
		}

		lt := C.duckdb_column_logical_type(
			&result,
			C.idx_t(0),
		)

		if C.duckdb_get_type_id(lt) == C.DUCKDB_TYPE_ENUM {
			sz := int(C.duckdb_enum_dictionary_size(lt))

			var values = make([]unsafe.Pointer, sz)
			for i := 0; i < sz; i++ {
				val := C.duckdb_enum_dictionary_value(lt, (C.idx_t)(i))
				values[i] = unsafe.Pointer(val)
			}
			enumCacheMtx.Lock()
			if _, ok := enumCache[sql]; !ok {
				// only set if not set to avoid memory leak
				enumCache[sql] = values
			}
			enumCacheMtx.Unlock()
		}

		return lt, nil
	}

	return f
}()
