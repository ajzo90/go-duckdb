package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"sync"
	"unsafe"
)

var chunkPool = sync.Pool{
	New: func() any {
		return &UDFDataChunk{}
	},
}

func chunkSize(chunk C.duckdb_data_chunk) int {
	return int(C.duckdb_data_chunk_get_size(chunk))
}

func acquireChunkFromVec(vecSize int, v C.duckdb_vector) *UDFDataChunk {
	c := _acquireChunk(vecSize, 1)
	c.Columns[0].init(vecSize, v)
	return c
}

func acquireChunk(vecSize int, chunk C.duckdb_data_chunk) *UDFDataChunk {
	cols := int(C.duckdb_data_chunk_get_column_count(chunk))
	c := _acquireChunk(vecSize, cols)
	for i := range c.Columns {
		c.Columns[i].init(vecSize, C.duckdb_data_chunk_get_vector(chunk, C.uint64_t(i)))
	}
	return c
}

func _acquireChunk(vecSize int, cols int) *UDFDataChunk {
	c := chunkPool.Get().(*UDFDataChunk)
	if cap(c.Columns) < cols {
		c.Columns = make([]Vector, cols)
	}
	c.Columns = c.Columns[:cols]
	c.Capacity = vecSize
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

type UDFDataChunk struct {
	Columns  []Vector
	Capacity int
}

type Vector struct {
	vector      C.duckdb_vector
	ptr         unsafe.Pointer
	childVec    *Vector
	pos         int
	uint64s     []uint64
	uint32s     []uint32
	uint16s     []uint16
	uint8s      []uint8
	float64s    []float64
	float32s    []float32
	bools       []bool
	uuids       []C.duckdb_hugeint
	listEntries []C.duckdb_list_entry
	bitmask     *C.uint64_t
}

func (d *Vector) AppendUInt64(v uint64) {
	d.uint64s[d.pos] = v
	d.pos++
}

func (d *Vector) ReserveListSize(newLength int) {
	C.duckdb_list_vector_set_size(d.vector, C.idx_t(newLength))
	C.duckdb_list_vector_reserve(d.vector, C.idx_t(newLength))
}

func (d *Vector) AppendListEntry(n int) {
	d.listEntries[d.pos] = C.duckdb_list_entry{
		offset: C.idx_t(d.childVec.pos),
		length: C.idx_t(n),
	}
	d.pos++
}

func (d *Vector) Child() *Vector {
	return d.childVec
}

func AppendBytes(vec *Vector, v []byte) {
	vec.AppendBytes(v)
}

func AppendUUID(vec *Vector, v []byte) {
	vec.uuids[vec.pos] = uuidToHugeInt(UUID(v))
	vec.pos++
}

func RawCopy[T any](vec *Vector, v []T) {
	copy((*[1 << 31]T)(vec.ptr)[:], v)
}

func (d *Vector) Append(v any) {
	switch x := v.(type) {
	case uint64:
		d.AppendUInt64(x)
	case uint32:
		d.AppendUInt32(x)
	case uint16:
		d.AppendUInt16(x)
	case uint8:
		d.AppendUInt8(x)
	case int64:
		d.AppendUInt64(uint64(x))
	case int32:
		d.AppendUInt32(uint32(x))
	case int16:
		d.AppendUInt16(uint16(x))
	case int8:
		d.AppendUInt8(uint8(x))
	case float64:
		d.AppendFloat64(x)
	case float32:
		d.AppendFloat32(x)
	case bool:
		d.AppendBool(x)
	case []byte:
		d.AppendBytes(x)
	default:
		panic("not supported")
	}
}

func (d *Vector) AppendUInt32(v uint32) {
	d.uint32s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendUInt16(v uint16) {
	d.uint16s[d.pos] = v
	d.pos++
}

func (d *Vector) GetSize() int {
	return d.pos
}

func (d *Vector) SetSize(n int) {
	for d.pos < n {
		d.AppendNull()
	}
}

func (d *Vector) AppendUInt8(v uint8) {
	d.uint8s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendFloat64(v float64) {
	d.float64s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendFloat32(v float32) {
	d.float32s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendBool(v bool) {
	d.bools[d.pos] = v
	d.pos++
}

func (d *Vector) appendBytes(v []byte) {
	sz := len(v)
	if sz > 0 {
		cstr := (*C.char)(unsafe.Pointer(&v[0]))
		C.duckdb_vector_assign_string_element_len(d.vector, C.uint64_t(d.pos), cstr, C.idx_t(sz))
	} else {
		C.duckdb_vector_assign_string_element_len(d.vector, C.uint64_t(d.pos), nil, C.idx_t(0))
	}
	d.pos++
}

func (d *Vector) AppendBytes(v []byte) {
	d.appendBytes(v)
}

func (d *Vector) Size() int {
	return d.pos
}

func (d *Vector) AppendNull() {
	C.duckdb_validity_set_row_invalid(d.bitmask, C.uint64_t(d.pos))
	d.pos++
}

func initVecSlice[T uint64 | uint32 | uint16 | uint8 | float64 | float32 | bool | [16]uint8 | C.duckdb_hugeint | C.duckdb_list_entry](sl *[]T, ptr unsafe.Pointer, sz int) {
	*sl = (*[1 << 31]T)(ptr)[:sz]
}

func (d *Vector) init(sz int, v C.duckdb_vector) {
	logicalType := C.duckdb_vector_get_column_type(v)
	duckdbType := C.duckdb_get_type_id(logicalType)
	C.duckdb_destroy_logical_type(&logicalType)
	d.pos = 0
	d.vector = v
	d.ptr = C.duckdb_vector_get_data(v)

	if duckdbType == C.DUCKDB_TYPE_LIST {
		initVecSlice(&d.listEntries, d.ptr, sz)
		d.childVec = acquireVector()
		d.childVec.init(sz, C.duckdb_list_vector_get_child(d.vector))
	} else {
		initVecSlice(&d.uint64s, d.ptr, sz)
		initVecSlice(&d.uint32s, d.ptr, sz)
		initVecSlice(&d.uint16s, d.ptr, sz)
		initVecSlice(&d.uint8s, d.ptr, sz)
		initVecSlice(&d.float64s, d.ptr, sz)
		initVecSlice(&d.float32s, d.ptr, sz)
		initVecSlice(&d.bools, d.ptr, sz)
		initVecSlice(&d.uuids, d.ptr, sz)
		C.duckdb_vector_ensure_validity_writable(v)
		d.bitmask = C.duckdb_vector_get_validity(v)
	}

}

func acquireVector() *Vector {
	return vectorPool.Get().(*Vector)
}
func releaseVector(v *Vector) {
	vectorPool.Put(v)
}

var vectorPool = sync.Pool{
	New: func() any {
		return &Vector{}
	},
}
