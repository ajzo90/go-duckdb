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


*/
import "C"

import (
	"database/sql/driver"
	"unsafe"
)

type SumState struct {
	Sum   int64
	Count int64
}

//export go_duckdb_aggregate_state_size
func go_duckdb_aggregate_state_size(info C.duckdb_function_info) C.idx_t {
	return C.idx_t(unsafe.Sizeof(SumState{}))
}

//export go_duckdb_aggregate_init
func go_duckdb_aggregate_init(info C.duckdb_function_info, state C.duckdb_aggregate_state) {
	var s = (*SumState)(unsafe.Pointer(state))
	s.Sum = 0
	s.Count = 0
}

//export go_duckdb_aggregate_destroy
func go_duckdb_aggregate_destroy(states *C.duckdb_aggregate_state, count C.idx_t) {

}

//export go_duckdb_aggregate_update
func go_duckdb_aggregate_update(info C.duckdb_function_info, input C.duckdb_data_chunk, states *C.duckdb_aggregate_state) {
	var row_count = C.duckdb_data_chunk_get_size(input)
	ch := acquireChunk(int(C.duckdb_vector_size()), input)
	defer releaseChunk(ch)

	input_data, _ := GetVector[int64](ch, 0)
	weight_data, _ := GetVector[int64](ch, 1)

	var aggs = (*[1 << 31]*SumState)(unsafe.Pointer(states))[:row_count:row_count]

	for i := range aggs {
		aggs[i].Sum += input_data[i] * weight_data[i]
		aggs[i].Count++
	}
}

//export go_duckdb_aggregate_combine
func go_duckdb_aggregate_combine(info C.duckdb_function_info, source *C.duckdb_aggregate_state, target *C.duckdb_aggregate_state, count C.idx_t) {
	var n = int(count)
	var s = (*[1 << 31]*SumState)(unsafe.Pointer(source))[:n:n]
	var t = (*[1 << 31]*SumState)(unsafe.Pointer(target))[:n:n]
	for i := range s {
		t[i].Sum += s[i].Sum
		t[i].Count += s[i].Count
	}
}

//export go_duckdb_aggregate_finalize
func go_duckdb_aggregate_finalize(info C.duckdb_function_info, source *C.duckdb_aggregate_state, result C.duckdb_vector, count C.idx_t, offset C.idx_t) {
	resVec := acquireVector(int(count), result)
	releaseVector(resVec)

	var s = (*[1 << 31]*SumState)(unsafe.Pointer(source))
	vv := vectorData[int64](resVec)[offset:]

	for i := 0; i < int(count); i++ {
		vv[i] = s[i].Sum
	}
}

func RegisterAggregateUDFConn(c driver.Conn, name string, f AggregateFunction) error {
	duckConn, err := getConn(c)
	if err != nil {
		return err
	}

	functionName := C.CString(name)
	defer C.free(unsafe.Pointer(functionName))

	function := C.duckdb_create_aggregate_function()
	C.duckdb_aggregate_function_set_name(function, functionName)

	var typ = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT)
	C.duckdb_aggregate_function_add_parameter(function, typ)
	C.duckdb_aggregate_function_add_parameter(function, typ)

	C.duckdb_aggregate_function_set_return_type(function, typ)
	C.duckdb_destroy_logical_type(&typ)

	C.duckdb_aggregate_function_set_functions(function,
		C.duckdb_aggregate_state_size(C.go_duckdb_aggregate_state_size),
		C.duckdb_aggregate_init_t(C.go_duckdb_aggregate_init),
		C.duckdb_aggregate_update_t(C.go_duckdb_aggregate_update),
		C.duckdb_aggregate_combine_t(C.go_duckdb_aggregate_combine),
		C.duckdb_aggregate_finalize_t(C.go_duckdb_aggregate_finalize),
	)

	status := C.duckdb_register_aggregate_function(duckConn.duckdbCon, function)
	if status != 0 {
		panic(status)
	}

	return nil
}

type AggregateFunction interface {
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
