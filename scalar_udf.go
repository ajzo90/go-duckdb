//go:build !no_duckdb_scalar_udf

package duckdb

// Related issues: https://golang.org/issue/19835, https://golang.org/issue/19837.

/*
#include <stdlib.h>
#include <duckdb.h>

void scalar_udf_callback(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
void scalar_udf_delete_callback(void *);

typedef void (*scalar_udf_callback_t)(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"unsafe"
)

type ScalarFunctionConfig struct {
	InputTypes      []string
	ResultType      string
	SpecialHandling bool
	Volatile        bool
}

type ScalarFunction interface {
	Config() ScalarFunctionConfig
	Exec(in *UDFDataChunk, out *Vector) error
}

//export scalar_udf_callback
func scalar_udf_callback(info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {

	infoX := C.duckdb_scalar_function_get_extra_info(info)
	scalarFunction := cMem.lookup((*ref)(infoX)).(ScalarFunction)

	var inputSize = chunkSize(input)
	var inputChunk = acquireChunk(inputSize, input)
	var outputChunk = acquireVector(inputSize, output)

	// todo: set out validity as intersection of validity

	err := scalarFunction.Exec(inputChunk, outputChunk)
	if err != nil {
		errstr := C.CString(err.Error())
		C.duckdb_scalar_function_set_error(info, errstr)
		C.free(unsafe.Pointer(errstr))
	}

	releaseVector(outputChunk)
	releaseChunk(inputChunk)
}

//export scalar_udf_delete_callback
func scalar_udf_delete_callback(data unsafe.Pointer) {
	cMem.free((*ref)(data))
}

var errScalarUDFNoName = fmt.Errorf("errScalarUDFNoName")

func RegisterScalarUDFConn(c driver.Conn, name string, function ScalarFunction) error {
	driverConn, err := getConn(c)
	if err != nil {
		return err
	} else if name == "" {
		return errScalarUDFNoName
	}
	functionName := C.CString(name)
	defer C.free(unsafe.Pointer(functionName))

	scalarFunction := C.duckdb_create_scalar_function()
	C.duckdb_scalar_function_set_name(scalarFunction, functionName)

	// Add input parameters.
	for _, inputType := range function.Config().InputTypes {
		sqlType := strings.ToUpper(inputType)
		logicalType, err := createLogicalFromSQLType(sqlType)
		if err != nil {
			return unsupportedTypeError(sqlType)
		}
		//C.duckdb_scalar_function_set_varargs(scalarFunction, logicalType)
		C.duckdb_scalar_function_add_parameter(scalarFunction, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
	}

	// Add result parameter.
	sqlType := strings.ToUpper(function.Config().ResultType)
	logicalType, err := createLogicalFromSQLType(sqlType)
	if err != nil {
		return unsupportedTypeError(sqlType)
	}
	C.duckdb_scalar_function_set_return_type(scalarFunction, logicalType)
	C.duckdb_destroy_logical_type(&logicalType)

	// Set the actual function.
	C.duckdb_scalar_function_set_function(scalarFunction, C.scalar_udf_callback_t(C.scalar_udf_callback))

	// Set data available during execution.
	C.duckdb_scalar_function_set_extra_info(
		scalarFunction,
		cMem.store(function),
		C.duckdb_delete_callback_t(C.scalar_udf_delete_callback),
	)

	if function.Config().Volatile {
		C.duckdb_scalar_function_set_volatile(scalarFunction)
	}

	// Register the function.
	state := C.duckdb_register_scalar_function(driverConn.duckdbCon, scalarFunction)
	C.duckdb_destroy_scalar_function(&scalarFunction)

	if state == C.DuckDBError {
		return fmt.Errorf("failed to register scalar UDF")
		//errDriver := C.duckdb_error_message(driverConn.duckdbCon)
		//return getError(errDriver, nil)
	}

	return nil
}

// RegisterScalarUDF registers a scalar UDF.
func RegisterScalarUDF(c *sql.Conn, name string, function ScalarFunction) error {
	// c.Raw exposes the underlying driver connection.
	err := c.Raw(func(anyConn any) error {
		conn, ok := anyConn.(driver.Conn)
		if !ok {
			return driver.ErrBadConn
		}
		return RegisterScalarUDFConn(conn, name, function)
	})
	return err
}

/*
https://github.com/duckdb/duckdb/pull/11786

typedef void (*duckdb_scalar_function_t)(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);


duckdb_scalar_function duckdb_create_scalar_function();
void duckdb_destroy_scalar_function(duckdb_scalar_function *scalar_function);
void duckdb_scalar_function_set_name(duckdb_scalar_function scalar_function, const char *name);
void duckdb_scalar_function_add_parameter(duckdb_scalar_function scalar_function, duckdb_logical_type type);
void duckdb_scalar_function_set_return_type(duckdb_scalar_function scalar_function, duckdb_logical_type type);
void duckdb_scalar_function_set_extra_info(duckdb_scalar_function scalar_function, void *extra_info,
													 duckdb_delete_callback_t destroy);
void duckdb_scalar_function_set_function(duckdb_scalar_function scalar_function,
												    duckdb_scalar_function_t function);
duckdb_state duckdb_register_scalar_function(duckdb_connection con, duckdb_scalar_function scalar_function);

void MyAddition(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	// get the total number of rows in this chunk
	idx_t input_size = duckdb_data_chunk_get_size(input);
	// extract the two input vectors
	duckdb_vector a = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector b = duckdb_data_chunk_get_vector(input, 1);
	// get the data pointers for the input vectors (both int64 as specified by the parameter types)
	auto a_data = (int64_t *) duckdb_vector_get_data(a);
	auto b_data = (int64_t *) duckdb_vector_get_data(b);
	auto result_data = (int64_t *) duckdb_vector_get_data(output);
	// get the validity vectors
	auto a_validity = duckdb_vector_get_validity(a);
	auto b_validity = duckdb_vector_get_validity(b);
	// if either a_validity or b_validity is defined there might be NULL values
	duckdb_vector_ensure_validity_writable(output);
	auto result_validity = duckdb_vector_get_validity(output);
	for(idx_t row = 0; row < input_size; row++) {
		if (duckdb_validity_row_is_valid(a_validity, row) && duckdb_validity_row_is_valid(b_validity, row)) {
			// not null - do the addition
			result_data[row] = a_data[row] + b_data[row];
		} else {
			// either a or b is NULL - set the result row to NULL
			duckdb_validity_set_row_invalid(result_validity, row);
		}
	}
}

static void CAPIRegisterAddition(duckdb_connection connection) {
	duckdb_state status;

	// create a scalar function
	auto function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "my_addition");

	// add a two bigint parameters
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_table_function_add_parameter(function, type);
	duckdb_table_function_add_parameter(function, type);

	// set the return type to bigint
	duckdb_scalar_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);

	// set up the function
	duckdb_scalar_function_set_function(function, MyAddition);

	// register and cleanup
        duckdb_register_scalar_function(connection, function);

	duckdb_destroy_scalar_function(&function);
}
*/
