package duckdb

/*
   	#include <stdlib.h>
   	#include <duckdb.h>
   	void replacement_scan_cb(duckdb_replacement_scan_info info, const char *table_name, void *data);
   	typedef const char cchar_t;
	void replacement_scan_destroy_data(void *);
*/
import "C"

import (
	"unsafe"
)

type ReplacementScanCallback func(tableName string) (string, []any, error)

func RegisterReplacementScan(connector *Connector, cb ReplacementScanCallback) {
	C.duckdb_add_replacement_scan(connector.db, C.duckdb_replacement_callback_t(C.replacement_scan_cb), cMem.store(cb), C.duckdb_delete_callback_t(C.replacement_scan_destroy_data))
}

//export replacement_scan_destroy_data
func replacement_scan_destroy_data(data unsafe.Pointer) {
	cMem.free((*ref)(data))
}

//export replacement_scan_cb
func replacement_scan_cb(info C.duckdb_replacement_scan_info, table_name *C.cchar_t, data *C.void) {
	scanner := cMem.lookup((*ref)(unsafe.Pointer(data))).(ReplacementScanCallback)
	tFunc, params, err := scanner(C.GoString(table_name))
	if err != nil {
		errstr := C.CString(err.Error())
		C.duckdb_replacement_scan_set_error(info, errstr)
		C.free(unsafe.Pointer(errstr))
		return
	}

	fNameStr := C.CString(tFunc)
	C.duckdb_replacement_scan_set_function_name(info, fNameStr)
	defer C.free(unsafe.Pointer(fNameStr))

	for _, v := range params {
		switch x := v.(type) {
		case string:
			str := C.CString(x)
			val := C.duckdb_create_varchar(str)
			C.duckdb_replacement_scan_add_parameter(info, val)
			C.free(unsafe.Pointer(str))
			C.duckdb_destroy_value(&val)
		case int64:
			val := C.duckdb_create_int64(C.int64_t(x))
			C.duckdb_replacement_scan_add_parameter(info, val)
			C.duckdb_destroy_value(&val)
		default:
			errstr := C.CString("invalid type")
			C.duckdb_replacement_scan_set_error(info, errstr)
			C.free(unsafe.Pointer(errstr))
			return
		}
	}
}
