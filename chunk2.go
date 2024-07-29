package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"sync"
	"unsafe"
)

type UDFDataChunk struct {
	Columns  []Vector
	Capacity int
	chunk    C.duckdb_data_chunk
}

type Vector struct {
	vector       C.duckdb_vector
	childVec     *Vector
	pos          int
	listCapacity int
	data         unsafe.Pointer
	bitmask      *C.uint64_t
}

func AppendUUID(d *Vector, v []byte) {
	Append(d, HugeInt(uuidToHugeInt(UUID(v))))
}

func AppendNull(d *Vector) {
	C.duckdb_validity_set_row_invalid(d.bitmask, C.uint64_t(d.pos))
	d.pos++
}

func AppendBytes(d *Vector, v []byte) {
	sz := len(v)
	if sz > 0 {
		cstr := (*C.char)(unsafe.Pointer(&v[0]))
		C.duckdb_vector_assign_string_element_len(d.vector, C.uint64_t(d.pos), cstr, C.idx_t(sz))
	} else {
		C.duckdb_vector_assign_string_element_len(d.vector, C.uint64_t(d.pos), nil, C.idx_t(0))
	}
	d.pos++
}

func vectorData[T any](vec *Vector) []T {
	return (*[1 << 31]T)(vec.data)[:]
}

func (d *Vector) Child() *Vector {
	return d.childVec
}

var chunkPool = sync.Pool{
	New: func() any {
		return &UDFDataChunk{}
	},
}

func chunkSize(chunk C.duckdb_data_chunk) int {
	return int(C.duckdb_data_chunk_get_size(chunk))
}

func (d *Vector) SetListSize(newLength int) {
	if d.childVec.listCapacity < newLength {
		d.ReserveListSize(max(newLength, 2048, d.childVec.listCapacity*2))
	}
	C.duckdb_list_vector_set_size(d.vector, C.idx_t(newLength))
}

func (d *Vector) ReserveListSize(newCapacity int) {
	if newCapacity < d.listCapacity {
		return
	}
	C.duckdb_list_vector_reserve(d.vector, C.idx_t(newCapacity))
	d.childVec.listCapacity = newCapacity
	d.childVec.data = C.duckdb_vector_get_data(d.childVec.vector)
	d.childVec.bitmask = C.duckdb_vector_get_validity(d.childVec.vector)
}

func (d *Vector) AppendListEntry(n int) {
	entry := C.duckdb_list_entry{
		offset: C.idx_t(d.childVec.pos),
		length: C.idx_t(n),
	}
	Append(d, entry)
}

func Append[T validTypes](vec *Vector, v T) {
	arr := (*[1 << 31]T)(vec.data)
	arr[vec.pos] = v
	vec.pos++
}

func AppendMany[T validTypes](vec *Vector, v []T) {
	vec.pos += rawCopy(vec, v)
}

func rawCopy[T any](vec *Vector, v []T) int {
	return copy(vectorData[T](vec)[vec.pos:], v)
}

func (d *Vector) init(sz int, v C.duckdb_vector, writable bool) {
	logicalType := C.duckdb_vector_get_column_type(v)
	duckdbType := C.duckdb_get_type_id(logicalType)
	C.duckdb_destroy_logical_type(&logicalType)
	d.pos = 0
	d.listCapacity = 0
	d.vector = v
	d.data = C.duckdb_vector_get_data(v)

	if duckdbType == C.DUCKDB_TYPE_LIST {
		d.childVec = acquireVector(sz, C.duckdb_list_vector_get_child(d.vector))
	} else if writable {
		C.duckdb_vector_ensure_validity_writable(v)
		d.bitmask = C.duckdb_vector_get_validity(v)
	}
}

func acquireVector(sz int, v C.duckdb_vector) *Vector {
	vec := vectorPool.Get().(*Vector)
	vec.init(sz, v, true)
	return vec
}

func releaseVector(v *Vector) {
	vectorPool.Put(v)
}

var vectorPool = sync.Pool{
	New: func() any {
		return &Vector{}
	},
}

func acquireChunk(vecSize int, chunk C.duckdb_data_chunk) *UDFDataChunk {
	cols := int(C.duckdb_data_chunk_get_column_count(chunk))
	c := chunkPool.Get().(*UDFDataChunk)
	c.chunk = chunk
	if cap(c.Columns) < cols {
		c.Columns = make([]Vector, cols)
	}
	c.Columns = c.Columns[:cols]
	c.Capacity = vecSize
	for i := range c.Columns {
		c.Columns[i].init(vecSize, C.duckdb_data_chunk_get_vector(chunk, C.uint64_t(i)), true)
	}
	return c
}

func releaseChunk(ch *UDFDataChunk) {
	for i := range ch.Columns {
		if ch.Columns[i].childVec != nil {
			releaseVector(ch.Columns[i].childVec)
			ch.Columns[i].childVec = nil
		}
	}
	chunkPool.Put(ch)
}

func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func s2b(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
