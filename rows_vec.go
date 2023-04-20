package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"io"
	"unsafe"
)

func getVec[T any](vector C.duckdb_vector) *[1 << 31]T {
	ptr := C.duckdb_vector_get_data(vector)
	return (*[1 << 31]T)(ptr)
}

func (r *rows) LoadVec() error {
	C.duckdb_destroy_data_chunk(&r.chunk)
	if r.chunkIdx == r.chunkCount {
		return io.EOF
	}
	r.chunk = C.duckdb_result_get_chunk(r.res, r.chunkIdx)
	r.chunkIdx++
	r.chunkRowCount = C.duckdb_data_chunk_get_size(r.chunk)
	r.chunkRowIdx = 0

	return nil
}

type VecScanner interface {
	U32Vec(colIdx int) ([]uint32, error)
	F32Vec(colIdx int) ([]float32, error)
	F64Vec(colIdx int) ([]float64, error)
	StringVec(colIdx int, buf [][]byte) ([][]byte, error)
	LoadVec() error
}

func (r *rows) U32Vec(colIdx int) ([]uint32, error) {
	return getGen[uint32](C.DUCKDB_TYPE_UINTEGER, r, colIdx)
}

func (r *rows) F32Vec(colIdx int) ([]float32, error) {
	return getGen[float32](C.DUCKDB_TYPE_FLOAT, r, colIdx)
}

func (r *rows) F64Vec(colIdx int) ([]float64, error) {
	return getGen[float64](C.DUCKDB_TYPE_DOUBLE, r, colIdx)
}

func (r *rows) StringVec(colIdx int, buf [][]byte) ([][]byte, error) {
	arr, err := getGen[duckdb_string_t](C.DUCKDB_TYPE_VARCHAR, r, colIdx)
	for _, s := range arr {
		var b []byte
		if s.length <= stringInlineLength {
			// inline data is stored from byte 4..16 (up to 12 bytes)
			b = C.GoBytes(unsafe.Pointer(&s.prefix), C.int(s.length))
		} else {
			// any longer strings are stored as a pointer in `ptr`
			b = C.GoBytes(unsafe.Pointer(s.ptr), C.int(s.length))
		}
		buf = append(buf, b)
	}
	return buf, err
}

func getGen[T any](typ C.duckdb_type, r *rows, colIdx int) ([]T, error) {
	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))

	ty := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&ty)
	typeId := C.duckdb_get_type_id(ty)

	switch typeId {
	case typ:
		return getVec[T](vector)[:r.chunkRowCount], nil
	default:
		return nil, errInvalidType
	}
}
