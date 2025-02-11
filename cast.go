package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
bool cast_udf_callback(duckdb_function_info info, idx_t count, duckdb_vector input, duckdb_vector output);
void cast_udf_delete_callback(void *);

typedef bool (*duckdb_cast_function_t)(duckdb_function_info info, idx_t count, duckdb_vector input,
                                       duckdb_vector output);

*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"unsafe"
)

type CastFunction interface {
	Config() CastFunctionConfig
	Exec(ctx *CastExecContext) error
}

type CastFunctionConfig struct {
	Source string
	Target string
	Cost   int
}

type CastExecContext struct {
	input     DuckdbVector
	output    DuckdbVector
	count     int
	isTryCast bool
}

func (ctx *CastExecContext) Input() DuckdbVector {
	return ctx.input
}
func (ctx *CastExecContext) Output() DuckdbVector {
	return ctx.output
}
func (ctx *CastExecContext) Count() int {
	return ctx.count
}

//export cast_udf_callback
func cast_udf_callback(info C.duckdb_function_info, count C.idx_t, input C.duckdb_vector, output C.duckdb_vector) C.bool {
	var isTryCast = C.duckdb_cast_function_get_cast_mode(info) == C.DUCKDB_CAST_TRY
	infoX := C.duckdb_cast_function_get_extra_info(info)
	castFunc := cMem.lookup((*ref)(infoX)).(CastFunction)
	var ctx = CastExecContext{input: input, output: output, count: int(count), isTryCast: isTryCast}
	if err := castFunc.Exec(&ctx); err != nil {
		errStr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errStr))
		C.duckdb_cast_function_set_error(info, errStr)
		return false
	}
	return true
}

//export cast_udf_delete_callback
func cast_udf_delete_callback(data unsafe.Pointer) {
	cMem.free((*ref)(data))
}

func RegisterType(c driver.Conn, name string, sql string) error {
	driverConn, err := getConn(c)
	if err != nil {
		return err
	}
	return RegisterTypeConn(driverConn, name, func() C.duckdb_logical_type {
		return Must(driverConn.sqlToLogical(sql))
	})
}

func CreateStructExample() C.duckdb_logical_type {
	var logicalTypes = []C.duckdb_logical_type{
		C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT),
		C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT),
		C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT),
		C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB),
	}

	var n = []string{"tag", "lo", "hi", "c"}

	var values = make([]unsafe.Pointer, 0)
	for _, name := range n {
		values = append(values, unsafe.Pointer(C.CString(name)))
	}

	strs := (**C.char)(malloc(values...))
	return C.duckdb_create_struct_type(&logicalTypes[0], strs, C.idx_t(len(n)))
}

func RegisterTypeConn(conn *conn, name string, typ func() C.duckdb_logical_type) error {
	logicalType := typ()

	defer C.duckdb_destroy_logical_type(&logicalType)

	typeName := C.CString(name)
	defer C.free(unsafe.Pointer(typeName))

	C.duckdb_logical_type_set_alias(logicalType, typeName)

	status := C.duckdb_register_logical_type(conn.duckdbCon, logicalType, nil)

	if status != C.DuckDBSuccess {
		return fmt.Errorf("failed to register type %s", name)
	}

	return nil

}

type Connection = C.duckdb_connection

func RegisterCastConn(conn *conn, function CastFunction) error {
	castFunc := C.duckdb_create_cast_function()

	//duckdb_cast_function_set_error
	//duckdb_cast_function_set_row_error
	//duckdb_destroy_cast_function

	cnf := function.Config()

	inputLogicalType, err := conn.sqlToLogical(cnf.Source)
	if err != nil {
		return unsupportedTypeError(cnf.Source)
	}
	C.duckdb_cast_function_set_source_type(castFunc, inputLogicalType)
	C.duckdb_destroy_logical_type(&inputLogicalType)

	targetLogicalType, err := conn.sqlToLogical(cnf.Target)
	if err != nil {
		return unsupportedTypeError(cnf.Target)
	}
	C.duckdb_cast_function_set_target_type(castFunc, targetLogicalType)
	C.duckdb_destroy_logical_type(&targetLogicalType)

	C.duckdb_cast_function_set_implicit_cast_cost(castFunc, C.int64_t(cnf.Cost))
	C.duckdb_cast_function_set_extra_info(
		castFunc,
		cMem.store(function),
		C.duckdb_delete_callback_t(C.cast_udf_delete_callback),
	)

	C.duckdb_cast_function_set_function(castFunc, C.duckdb_cast_function_t(C.cast_udf_callback))

	res := C.duckdb_register_cast_function(conn.duckdbCon, castFunc)

	if res != C.DuckDBSuccess {
		return fmt.Errorf("failed to register cast")
	}

	return nil
}

func RegisterCast(c driver.Conn, function CastFunction) error {
	driverConn, err := getConn(c)
	if err != nil {
		return err
	}
	return RegisterCastConn(driverConn, function)
}
