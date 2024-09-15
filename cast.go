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
	return castFunc.Exec(&ctx) == nil
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
	return RegisterTypeConn(driverConn.duckdbCon, name, sql)
}

func RegisterTypeConn(duckdbCon C.duckdb_connection, name string, sql string) error {

	typeName := C.CString(name)
	defer C.free(unsafe.Pointer(typeName))

	logicalType, err := sqlToLogical(sql)
	if err != nil {
		return err
	}
	defer C.duckdb_destroy_logical_type(&logicalType)

	C.duckdb_logical_type_set_alias(logicalType, typeName)

	status := C.duckdb_register_logical_type(duckdbCon, logicalType, nil)

	if status != C.DuckDBSuccess {
		return fmt.Errorf("failed to register type %s", name)
	}

	return nil

}

func RegisterCastConn(duckdbCon C.duckdb_connection, function CastFunction) error {
	castFunc := C.duckdb_create_cast_function()

	cnf := function.Config()

	inputLogicalType, err := sqlToLogical(cnf.Source)
	if err != nil {
		return unsupportedTypeError(cnf.Source)
	}
	C.duckdb_cast_function_set_source_type(castFunc, inputLogicalType)
	C.duckdb_destroy_logical_type(&inputLogicalType)

	targetLogicalType, err := sqlToLogical(cnf.Target)
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

	res := C.duckdb_register_cast_function(duckdbCon, castFunc)

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
	return RegisterCastConn(driverConn.duckdbCon, function)
}
