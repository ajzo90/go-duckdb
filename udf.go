package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

void udf_bind(duckdb_bind_info info);

void udf_init(duckdb_init_info info);

void udf_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837

typedef void (*init)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*bind)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19835
*/
import "C"

import (
	"database/sql"
	"reflect"
	"unsafe"
)

type (
	ColumnName struct {
		Name string
		V    any
	}
	TableFunction interface {
		GetArguments() []any
		BindArguments(args ...interface{}) []ColumnName
		GetRow() []any
	}
	TableFunctionVec interface {
		GetRows(int) ([]any, int)
	}
)

//var tableBinds = []func(info C.duckdb_function_info, output C.duckdb_data_chunk)([]struct{name string, type duckdb_logical_type}){}

var tableFuncs = []TableFunction{}

//export udf_bind
func udf_bind(info C.duckdb_bind_info) {
	extra_info := C.duckdb_bind_get_extra_info(info)
	tfunc := tableFuncs[*(*int)(extra_info)]

	value := C.duckdb_bind_get_parameter(info, C.ulonglong(0))
	arg0, err := getValue(C.DUCKDB_TYPE_BIGINT, value)
	if err != nil {
		errstr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_bind_set_error(info, errstr)
		return
	}

	returnvalues := tfunc.BindArguments(arg0)

	for _, v := range returnvalues {
		t, err := getDuckdbTypeFromValue(v.V)
		if err != nil {
			errstr := C.CString(err.Error())
			defer C.free(unsafe.Pointer(errstr))
			C.duckdb_bind_set_error(info, errstr)
			return
		}

		colName := C.CString(v.Name)
		defer C.free(unsafe.Pointer(colName))
		C.duckdb_bind_add_result_column(info, colName, t)
	}
}

//export udf_init
func udf_init(info C.duckdb_init_info) {
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

//export udf_callback
func udf_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	extra_info := C.duckdb_function_get_extra_info(info)
	tfunc := tableFuncs[*(*int)(extra_info)]

	columnCount := int(C.duckdb_data_chunk_get_column_count(output))
	vectors := make([]vector, columnCount)
	var err error
	for i := 0; i < columnCount; i++ {
		duckdbVector := C.duckdb_data_chunk_get_vector(output, C.ulonglong(i))
		t := C.duckdb_vector_get_column_type(duckdbVector)
		if err = vectors[i].init(t, i); err != nil {
			break
		}
		vectors[i].duckdbVector = duckdbVector
		vectors[i].getChildVectors(duckdbVector)
	}
	if err != nil {
		errstr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_function_set_error(info, errstr)
		return
	}

	maxSize := int(C.duckdb_vector_size())

	if tf, ok := tfunc.(TableFunctionVec); ok {
		chunk, maxSz := tf.GetRows(maxSize)
		for j, val := range chunk {
			vec := vectors[j]
			if sl, ok := val.([]int64); ok {
				regVec(sl, &vec)
			} else if sl, ok := val.([]uint64); ok {
				regVec(sl, &vec)
			} else if sl, ok := val.([]float64); ok {
				regVec(sl, &vec)
			} else if sl, ok := val.([]bool); ok {
				regVec(sl, &vec)
			} else if sl, ok := val.([][]byte); ok {
				for rowIdx, v := range sl {
					cStr := C.CString(b2s(v))
					C.duckdb_vector_assign_string_element_len(vec.duckdbVector, C.ulonglong(rowIdx), cStr, C.idx_t(len(v)))
					C.free(unsafe.Pointer(cStr))
				}
			} else {
				panic(1)
			}
		}
		C.duckdb_data_chunk_set_size(output, C.ulonglong(maxSz))
		return
	}

	for i := 0; i < maxSize; i++ {
		nextResults := tfunc.GetRow()
		if nextResults == nil {
			break
		}
		for j, val := range nextResults {
			vec := vectors[j]

			// Ensure the types match before adding to the vector
			v, err := vec.tryCast(val)
			if err != nil {
				cerr := columnError(err, j+1)
				errstr := C.CString(cerr.Error())
				defer C.free(unsafe.Pointer(errstr))
				C.duckdb_function_set_error(info, errstr)
			}
			vec.fn(&vec, C.ulonglong(i), v)
			C.duckdb_data_chunk_set_size(output, C.ulonglong(i+1))
		}
	}
}

func regVec[T int64 | uint64 | float64 | bool](sl []T, vec *vector) {
	ptr := C.duckdb_vector_get_data(vec.duckdbVector)
	xs := (*[1 << 31]T)(ptr)
	for i, v := range sl {
		xs[i] = v
	}
}

func RegisterTableUDF(c *sql.Conn, name string, function TableFunction) error {
	err := c.Raw(func(dconn any) error {
		ddconn := dconn.(*conn)
		name := C.CString(name)
		defer C.free(unsafe.Pointer(name))

		extra_info := C.malloc(C.ulong(1 * unsafe.Sizeof(int(0))))
		*(*int)(extra_info) = len(tableFuncs)
		tableFuncs = append(tableFuncs, function)

		tableFunction := C.duckdb_create_table_function()
		C.duckdb_table_function_set_name(tableFunction, name)
		C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind))
		C.duckdb_table_function_set_init(tableFunction, C.init(C.udf_init))
		C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_callback))
		C.duckdb_table_function_set_extra_info(tableFunction, extra_info, C.duckdb_delete_callback_t(C.free))

		argumentvalues := function.GetArguments()

		for _, v := range argumentvalues {
			argtype, err := getDuckdbTypeFromValue(v)
			if err != nil {
				return err
			}
			C.duckdb_table_function_add_parameter(tableFunction, argtype)
		}

		state := C.duckdb_register_table_function(ddconn.duckdbCon, tableFunction)
		if state != 0 {
			return invalidTableFunctionError()
		}
		return nil
	})
	return err
}

func getDuckdbTypeFromValue(v any) (C.duckdb_logical_type, error) {
	switch v.(type) {
	case int64:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), nil
	case string:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), nil
	default:
		return C.duckdb_logical_type(nil), unsupportedTypeError(reflect.TypeOf(v).String())
	}
}

func getValue(t C.duckdb_type, v C.duckdb_value) (any, error) {
	switch t {
	case C.DUCKDB_TYPE_BIGINT:
		return int64(C.duckdb_get_int64(v)), nil
	case C.DUCKDB_TYPE_VARCHAR:
		str := C.duckdb_get_varchar(v)
		ret := C.GoString(str)
		C.duckdb_free(unsafe.Pointer(str))
		return ret, nil
	default:
		return nil, unsupportedTypeError(reflect.TypeOf(v).String())
	}
}
