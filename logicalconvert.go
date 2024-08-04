package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

*/
import "C"

import (
	"fmt"
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
	//defer C.duckdb_disconnect(&con)

	return func(sql string) (C.duckdb_logical_type, error) {

		//createLogicalFromSQLType(strings.ToUpper(sql))
		q := fmt.Sprintf("SELECT CAST(NULL AS %s)", sql)
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

		return lt, nil
	}
}()
