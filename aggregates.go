package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>


idx_t go_duckdb_aggregate_state_size(duckdb_function_info info);
void go_duckdb_aggregate_init(duckdb_function_info info, duckdb_aggregate_state state);
void go_duckdb_aggregate_destroy(duckdb_aggregate_state *states, idx_t count);
void go_duckdb_aggregate_update(duckdb_function_info info, duckdb_data_chunk input, duckdb_aggregate_state *states);
void go_duckdb_aggregate_combine(duckdb_function_info info, duckdb_aggregate_state *source, duckdb_aggregate_state *target, idx_t count);
void go_duckdb_aggregate_finalize(duckdb_function_info info, duckdb_aggregate_state *source, duckdb_vector result, idx_t count, idx_t offset);
void go_duckdb_aggregate_delete_callback(void *);

*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"unsafe"
)

type AggregateFunctionConfig struct {
	InputTypes      []string
	ResultType      string
	SpecialHandling bool
}

type AggregateFunction[StateType any] interface {
	Config() AggregateFunctionConfig
	Init(*StateType)
	Update([]*StateType, *UDFDataChunk)
	Combine(source, target []*StateType)
	Finalize([]*StateType, *Vector)
}

type aggFuncInternal struct {
	initFn     func(state C.duckdb_aggregate_state)
	updateFn   func(input C.duckdb_data_chunk, states *C.duckdb_aggregate_state)
	combineFn  func(source *C.duckdb_aggregate_state, target *C.duckdb_aggregate_state, count C.idx_t)
	finalizeFn func(source *C.duckdb_aggregate_state, result C.duckdb_vector, count C.idx_t, offset C.idx_t)
	size       int
}

//export go_duckdb_aggregate_state_size
func go_duckdb_aggregate_state_size(info C.duckdb_function_info) C.idx_t {
	return C.idx_t(internal(info).size)
}

func go_duckdb_aggregate_set_error(info C.duckdb_function_info, err error) {
	errstr := C.CString(err.Error())
	C.duckdb_aggregate_function_set_error(info, errstr)
	C.free(unsafe.Pointer(errstr))
}

func internal(info C.duckdb_function_info) *aggFuncInternal {
	infoX := C.duckdb_aggregate_function_get_extra_info(info)
	return cMem.lookup((*ref)(infoX)).(*aggFuncInternal)
}

//export go_duckdb_aggregate_init
func go_duckdb_aggregate_init(info C.duckdb_function_info, state C.duckdb_aggregate_state) {
	internal(info).initFn(state)
}

//export go_duckdb_aggregate_destroy
func go_duckdb_aggregate_destroy(states *C.duckdb_aggregate_state, count C.idx_t) {

}

//export go_duckdb_aggregate_delete_callback
func go_duckdb_aggregate_delete_callback(data unsafe.Pointer) {
	cMem.free((*ref)(data))
}

//export go_duckdb_aggregate_update
func go_duckdb_aggregate_update(info C.duckdb_function_info, input C.duckdb_data_chunk, states *C.duckdb_aggregate_state) {
	internal(info).updateFn(input, states)
}

//export go_duckdb_aggregate_combine
func go_duckdb_aggregate_combine(info C.duckdb_function_info, source *C.duckdb_aggregate_state, target *C.duckdb_aggregate_state, count C.idx_t) {
	internal(info).combineFn(source, target, count)
}

//export go_duckdb_aggregate_finalize
func go_duckdb_aggregate_finalize(info C.duckdb_function_info, source *C.duckdb_aggregate_state, result C.duckdb_vector, count C.idx_t, offset C.idx_t) {
	internal(info).finalizeFn(source, result, count, offset)
}

func RegisterAggregateUDFConn[StateType any](c driver.Conn, name string, f AggregateFunction[StateType]) error {
	duckConn, err := getConn(c)
	if err != nil {
		return err
	}

	functionName := C.CString(name)
	defer C.free(unsafe.Pointer(functionName))

	function := C.duckdb_create_aggregate_function()
	C.duckdb_aggregate_function_set_name(function, functionName)

	var conf = f.Config()

	// Add input parameters.
	for _, inputType := range conf.InputTypes {
		sqlType := strings.ToUpper(inputType)
		logicalType, err := createLogicalFromSQLType(sqlType)
		if err != nil {
			return unsupportedTypeError(sqlType)
		}
		C.duckdb_aggregate_function_add_parameter(function, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
	}

	// Add result parameter.
	sqlType := strings.ToUpper(conf.ResultType)
	logicalType, err := createLogicalFromSQLType(sqlType)
	if err != nil {
		return unsupportedTypeError(sqlType)
	}
	C.duckdb_aggregate_function_set_return_type(function, logicalType)
	C.duckdb_destroy_logical_type(&logicalType)

	if conf.SpecialHandling {
		C.duckdb_aggregate_function_set_special_handling(function)
	}

	C.duckdb_aggregate_function_set_functions(function,
		C.duckdb_aggregate_state_size(C.go_duckdb_aggregate_state_size),
		C.duckdb_aggregate_init_t(C.go_duckdb_aggregate_init),
		C.duckdb_aggregate_update_t(C.go_duckdb_aggregate_update),
		C.duckdb_aggregate_combine_t(C.go_duckdb_aggregate_combine),
		C.duckdb_aggregate_finalize_t(C.go_duckdb_aggregate_finalize),
	)

	C.duckdb_aggregate_function_set_destructor(function, C.duckdb_aggregate_destroy_t(C.go_duckdb_aggregate_destroy))

	var internal = &aggFuncInternal{
		size: int(unsafe.Sizeof(*new(StateType))),
		initFn: func(state C.duckdb_aggregate_state) {
			f.Init((*StateType)(unsafe.Pointer(state)))
		},
		updateFn: func(input C.duckdb_data_chunk, states *C.duckdb_aggregate_state) {
			var n = C.duckdb_data_chunk_get_size(input)
			ch := acquireChunk(int(C.duckdb_vector_size()), input)
			defer releaseChunk(ch)

			sl := (*[1 << 31]*StateType)(unsafe.Pointer(states))[:n:n]
			f.Update(sl, ch)
		},
		combineFn: func(source *C.duckdb_aggregate_state, target *C.duckdb_aggregate_state, count C.idx_t) {
			var n = int(count)
			var s = (*[1 << 31]*StateType)(unsafe.Pointer(source))[:n:n]
			var t = (*[1 << 31]*StateType)(unsafe.Pointer(target))[:n:n]

			f.Combine(s, t)
		},
		finalizeFn: func(source *C.duckdb_aggregate_state, result C.duckdb_vector, count C.idx_t, offset C.idx_t) {
			resVec := acquireVector(int(count), result)
			releaseVector(resVec)

			var s = (*[1 << 31]*StateType)(unsafe.Pointer(source))

			f.Finalize(s[offset:][:count], resVec)
		},
	}

	C.duckdb_aggregate_function_set_extra_info(function, cMem.store(internal), C.duckdb_delete_callback_t(C.go_duckdb_aggregate_delete_callback))

	status := C.duckdb_register_aggregate_function(duckConn.duckdbCon, function)
	if status != C.DuckDBSuccess {
		return fmt.Errorf("failed to register aggregate function")
	}

	return nil
}

/*
//! Returns the aggregate state size
typedef idx_t (*duckdb_aggregate_state_size)(duckdb_function_info info);
//! Initialize the aggregate state
typedef void (*duckdb_aggregate_init_t)(duckdb_function_info info, duckdb_aggregate_state state);
//! Destroy aggregate state (optional)
typedef void (*duckdb_aggregate_destroy_t)(duckdb_aggregate_state *states, idx_t count);
//! Update a set of aggregate states with new values
typedef void (*duckdb_aggregate_update_t)(duckdb_function_info info, duckdb_data_chunk input,
                                          duckdb_aggregate_state *states);
//! Combine aggregate states
typedef void (*duckdb_aggregate_combine_t)(duckdb_function_info info, duckdb_aggregate_state *source,
                                           duckdb_aggregate_state *target, idx_t count);
//! Finalize aggregate states into a result vector
typedef void (*duckdb_aggregate_finalize_t)(duckdb_function_info info, duckdb_aggregate_state *source,
                                            duckdb_vector result, idx_t count, idx_t offset);
*/

/*
duckdb_aggregate_function duckdb_create_aggregate_function();
void duckdb_destroy_aggregate_function(duckdb_aggregate_function *aggregate_function);
void duckdb_aggregate_function_set_name(duckdb_aggregate_function aggregate_function, const char *name);
void duckdb_aggregate_function_add_parameter(duckdb_aggregate_function aggregate_function,
                                                        duckdb_logical_type type);
void duckdb_aggregate_function_set_return_type(duckdb_aggregate_function aggregate_function,
                                                          duckdb_logical_type type);
void duckdb_aggregate_function_set_functions(duckdb_aggregate_function aggregate_function,
                                                        duckdb_aggregate_state_size state_size,
                                                        duckdb_aggregate_init_t state_init,
                                                        duckdb_aggregate_update_t update,
                                                        duckdb_aggregate_combine_t combine,
                                                        duckdb_aggregate_finalize_t finalize);
void duckdb_aggregate_function_set_destructor(duckdb_aggregate_function aggregate_function,
                                                         duckdb_aggregate_destroy_t destroy);
duckdb_state duckdb_register_aggregate_function(duckdb_connection con,
                                                           duckdb_aggregate_function aggregate_function);
void duckdb_aggregate_function_set_special_handling(duckdb_aggregate_function aggregate_function);
void duckdb_aggregate_function_set_extra_info(duckdb_aggregate_function aggregate_function, void *extra_info,
                                                         duckdb_delete_callback_t destroy);
void *duckdb_aggregate_function_get_extra_info(duckdb_function_info info);
void duckdb_aggregate_function_set_error(duckdb_function_info info, const char *error);
*/
