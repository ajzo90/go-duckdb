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

type VecScanner interface {
	U8Vec(colIdx int) ([]uint8, error)
	I8Vec(colIdx int) ([]int8, error)
	I16Vec(colIdx int) ([]int16, error)
	U16Vec(colIdx int) ([]uint16, error)
	U32Vec(colIdx int) ([]uint32, error)
	U64Vec(colIdx int) ([]uint64, error)
	U64ListVec(colIdx int, buf [][]uint64) ([][]uint64, error)
	I64Vec(colIdx int) ([]int64, error)
	I32Vec(colIdx int) ([]int32, error)
	U32ListVec(colIdx int, buf [][]uint32) ([][]uint32, error)
	I32ListVec(colIdx int, buf [][]int32) ([][]int32, error)
	I64ListVec(colIdx int, buf [][]int64) ([][]int64, error)
	BigIntVec(colIdx int, buf []int64) ([]int64, error)
	F32Vec(colIdx int) ([]float32, error)
	F64Vec(colIdx int) ([]float64, error)
	F32ListVec(colIdx int, buf [][]float32) ([][]float32, error)
	F64ListVec(colIdx int, buf [][]float64) ([][]float64, error)
	StringVec(colIdx int, buf [][]byte) ([][]byte, error)
	StrListVec(colIdx int, buf [][][]byte) ([][][]byte, error)
	BoolVec(colIdx int) ([]bool, error)
	BoolListVec(colIdx int, buf [][]bool) ([][]bool, error)
	TsVec(colIdx int, buf []time.Time) ([]time.Time, error)
	DateVec(colIdx int, buf []time.Time) ([]time.Time, error)
	LoadVec() error
	NumValues() int
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

func (r *rows) NumValues() int {
	return int(r.chunkRowCount)
}

func (r *rows) BigIntVec(colIdx int, vec []int64) ([]int64, error) {
	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))
	vec = vec[:0]
	for i := 0; i < int(r.chunkRowCount); i++ {
		hi := get[C.duckdb_hugeint](vector, C.idx_t(i))
		vec = append(vec, hugeIntToNative(hi).Int64())
	}
	return vec, nil
}

func (r *rows) U16Vec(colIdx int) ([]uint16, error) {
	return getGen[uint16](C.DUCKDB_TYPE_USMALLINT, r, colIdx)
}

func (r *rows) I8Vec(colIdx int) ([]int8, error) {
	return getGen[int8](C.DUCKDB_TYPE_TINYINT, r, colIdx)
}

func (r *rows) I16Vec(colIdx int) ([]int16, error) {
	return getGen[int16](C.DUCKDB_TYPE_SMALLINT, r, colIdx)
}

func (r *rows) U64Vec(colIdx int) ([]uint64, error) {
	return getGen[uint64](C.DUCKDB_TYPE_UBIGINT, r, colIdx)
}

func (r *rows) U32Vec(colIdx int) ([]uint32, error) {
	return getGen[uint32](C.DUCKDB_TYPE_UINTEGER, r, colIdx)
}

func (r *rows) U32ListVec(colId int, buf [][]uint32) ([][]uint32, error) {
	return listVec[uint32](r, colId, C.DUCKDB_TYPE_UINTEGER, buf)
}

func (r *rows) I64ListVec(colId int, buf [][]int64) ([][]int64, error) {
	return listVec[int64](r, colId, C.DUCKDB_TYPE_BIGINT, buf)
}

func (r *rows) F64ListVec(colId int, buf [][]float64) ([][]float64, error) {
	return listVec[float64](r, colId, C.DUCKDB_TYPE_DOUBLE, buf)
}

func (r *rows) F32ListVec(colId int, buf [][]float32) ([][]float32, error) {
	return listVec[float32](r, colId, C.DUCKDB_TYPE_FLOAT, buf)
}

func (r *rows) I32Vec(colIdx int) ([]int32, error) {
	return getGen[int32](C.DUCKDB_TYPE_INTEGER, r, colIdx)
}

func (r *rows) I32ListVec(colId int, buf [][]int32) ([][]int32, error) {
	return listVec[int32](r, colId, C.DUCKDB_TYPE_INTEGER, buf)
}

func (r *rows) U64ListVec(colId int, buf [][]uint64) ([][]uint64, error) {
	return listVec[uint64](r, colId, C.DUCKDB_TYPE_UBIGINT, buf)
}

func (r *rows) U8Vec(colIdx int) ([]uint8, error) {
	return getGen[uint8](C.DUCKDB_TYPE_UTINYINT, r, colIdx)
}

func (r *rows) BoolVec(colIdx int) ([]bool, error) {
	return getGen[bool](C.DUCKDB_TYPE_BOOLEAN, r, colIdx)
}

func (r *rows) BoolListVec(colId int, buf [][]bool) ([][]bool, error) {
	return listVec[bool](r, colId, C.DUCKDB_TYPE_BOOLEAN, buf)
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
	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))
	arr, err := getGen2[duckdb_string_t](C.DUCKDB_TYPE_VARCHAR, int(r.chunkRowCount), vector)
	validity := C.duckdb_vector_get_validity(vector)
	for i, s := range arr {
		if isValid(validity, i) {
			buf = append(buf, toStr(s))
		} else {
			buf = append(buf, nil)
		}
	}
	return buf, err
}

func getVec[T any](vector C.duckdb_vector) *[1 << 31]T {
	ptr := C.duckdb_vector_get_data(vector)
	return (*[1 << 31]T)(ptr)
}

func getGen[T any](typ C.duckdb_type, r *rows, colIdx int) ([]T, error) {
	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))
	return getGen2[T](typ, int(r.chunkRowCount), vector)
}

func getGen2[T any](typ C.duckdb_type, n int, vector C.duckdb_vector) ([]T, error) {
	ty := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&ty)

	switch C.duckdb_get_type_id(ty) {
	case typ:
		return getVec[T](vector)[:n], nil
	default:
		return nil, errInvalidType
	}
}

func (r *rows) StrListVec(colIdx int, buf [][][]byte) ([][][]byte, error) {
	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))
	data := C.duckdb_list_vector_get_child(vector)
	entries := getVec[duckdb_list_entry_t](vector)[:r.chunkRowCount]

	if cap(buf) < len(entries) {
		buf = make([][][]byte, len(entries))
	}
	buf = buf[:len(entries)]

	full, err := getGen2[duckdb_string_t](C.DUCKDB_TYPE_VARCHAR, 1<<31, data)
	if err != nil {
		return nil, err
	}

	for i, entry := range entries {
		buf[i] = buf[i][:0]
		for _, v := range full[entry.offset : entry.offset+entry.length] {
			buf[i] = append(buf[i], toStr(v))
		}
	}
	return buf, nil
}

func listVec[T any](r *rows, colIdx int, typ C.duckdb_type, buf [][]T) ([][]T, error) {
	vector := C.duckdb_data_chunk_get_vector(r.chunk, C.idx_t(colIdx))
	data := C.duckdb_list_vector_get_child(vector)
	entries := getVec[duckdb_list_entry_t](vector)[:r.chunkRowCount]

	if cap(buf) < len(entries) {
		buf = make([][]T, len(entries))
	}
	buf = buf[:len(entries)]

	full, err := getGen2[T](typ, 1<<31, data)
	if err != nil {
		return nil, err
	}

	for i, entry := range entries {
		buf[i] = full[entry.offset : entry.offset+entry.length]
	}
	return buf, nil
}

//; idx_t ; bool is_valid = validity_mask[entry_idx] & (1 « idx_in_entry);

func isValid(vec *C.uint64_t, rowIdx int) bool {
	var entry_idx = rowIdx / 64
	var idx_in_entry = rowIdx % 64
	var vect = (*[1 << 31]uint64)(unsafe.Pointer(vec))
	return vect[entry_idx]&(1<<idx_in_entry) != 0
}

func toStr(v duckdb_string_t) []byte {
	if v.length == 0 {
		return nil
	} else if v.length <= stringInlineLength {
		// inline data is stored from byte 4..16 (up to 12 bytes)
		return (*[1 << 31]byte)(unsafe.Pointer(&v.prefix))[:v.length]
	} else {
		// any longer strings are stored as a pointer in `ptr`
		return (*[1 << 31]byte)(unsafe.Pointer(v.ptr))[:v.length]
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
