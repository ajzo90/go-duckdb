package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"io"
	"time"
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
	U8Vec(colIdx int) ([]uint8, error)
	U32Vec(colIdx int) ([]uint32, error)
	I64Vec(colIdx int) ([]int64, error)
	F32Vec(colIdx int) ([]float32, error)
	F64Vec(colIdx int) ([]float64, error)
	StringVec(colIdx int, buf [][]byte) ([][]byte, error)
	BoolVec(colIdx int) ([]bool, error)
	TsVec(colIdx int, buf []time.Time) ([]time.Time, error)
	DateVec(colIdx int, buf []time.Time) ([]time.Time, error)
	LoadVec() error
}

func (r *rows) U32Vec(colIdx int) ([]uint32, error) {
	return getGen[uint32](C.DUCKDB_TYPE_UINTEGER, r, colIdx)
}

func (r *rows) U8Vec(colIdx int) ([]uint8, error) {
	return getGen[uint8](C.DUCKDB_TYPE_UTINYINT, r, colIdx)
}

func (r *rows) BoolVec(colIdx int) ([]bool, error) {
	return getGen[bool](C.DUCKDB_TYPE_BOOLEAN, r, colIdx)
}

func (r *rows) I64Vec(colIdx int) ([]int64, error) {
	return getGen[int64](C.DUCKDB_TYPE_BIGINT, r, colIdx)
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

func (r *rows) TsVec(colIdx int, buf []time.Time) ([]time.Time, error) {
	dates, err := getGen[C.duckdb_timestamp](C.DUCKDB_TYPE_TIMESTAMP_S, r, colIdx)
	if err != nil {
		return nil, err
	}
	for _, v := range dates {
		buf = append(buf, time.Unix(int64(v.micros), 0).UTC())
	}
	return buf, nil
}

func (r *rows) DateVec(colIdx int, buf []time.Time) ([]time.Time, error) {

	dates, err := getGen[C.duckdb_date](C.DUCKDB_TYPE_DATE, r, colIdx)
	if err != nil {
		return nil, err
	}

	for _, v := range dates {
		v := C.duckdb_from_date(v)
		buf = append(buf, time.Date(int(v.year), time.Month(v.month), int(v.day), 0, 0, 0, 0, time.UTC))
	}

	return buf, nil
}

//func (r *rows) StrListVec(colIdx int, buf [][][]byte) ([][][]byte, error) {
//	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))
//	data := C.duckdb_list_vector_get_child(vector)
//	entries := getVec[duckdb_list_entry_t](vector)[:r.chunkRowCount]
//
//	for _, entry := range entries{
//		converted := make([]any, 0, entry.length)
//		for i := entry.offset; i < entry.offset+entry.length; i++ {
//			value, err := scan(data, i)
//			if err != nil {
//				return nil, err
//			}
//			converted = append(converted, value)
//		}
//	}
//}
