package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"fmt"
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
	childVecs    []*Vector
	pos          int
	listCapacity int
	data         unsafe.Pointer
	bitmask      *C.uint64_t
	cap          int
}

type Vector2 struct {
	vector C.duckdb_vector
}

func (d *Vector) Validity(n int) []uint64 {
	return validity(d.vector, n)
}

func AppendUUID(d *Vector, v []byte) {
	Append(d, HugeInt(uuidToHugeInt(UUID(v))))
}

func AppendNull(d *Vector) {
	C.duckdb_validity_set_row_invalid(d.bitmask, C.uint64_t(d.pos))
	d.pos++
}

func SetNull(d *Vector, i int) {
	C.duckdb_validity_set_row_invalid(d.bitmask, C.uint64_t(i))
}

func SetValidity(d *Vector, validity []uint64) {
	var out = (*[1 << 31]uint64)(unsafe.Pointer(d.bitmask))[:len(validity)]
	copy(out, validity)
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

func VectorData[T any](vec *Vector) []T {
	return (*[1 << 31]T)(vec.data)[:]
}

func ValidVectorData[T any](vec *Vector, n int) []T {
	validity := vec.Validity(n)
	data := (*[1 << 31]T)(vec.data)[:n]
	zeroValidity(data, validity)
	return data
}

func (d *Vector) Childs() []*Vector {
	return d.childVecs
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
	if d.childVecs[0].listCapacity < newLength {
		d.ReserveListSize(max(newLength, 2048, d.childVecs[0].listCapacity*2))
	}
	C.duckdb_list_vector_set_size(d.vector, C.idx_t(newLength))
}

func (d *Vector) ReserveListSize(newCapacity int) {
	if newCapacity < d.listCapacity {
		return
	}
	C.duckdb_list_vector_reserve(d.vector, C.idx_t(newCapacity))
	for _, v := range d.childVecs {
		v.listCapacity = newCapacity
		v.cap = newCapacity
		//v.vector = C.duckdb_list_vector_get_child(d.vector)
		v.data = C.duckdb_vector_get_data(v.vector)
		v.bitmask = C.duckdb_vector_get_validity(v.vector)
		for _, vv := range v.childVecs {
			vv.cap = newCapacity
			vv.data = C.duckdb_vector_get_data(vv.vector)
			vv.bitmask = C.duckdb_vector_get_validity(vv.vector)
		}
	}
}

func (d *Vector) AppendListEntryRaw(offset, n int) {
	entry := C.duckdb_list_entry{
		offset: C.idx_t(offset),
		length: C.idx_t(n),
	}
	Append(d, entry)
}

func (d *Vector) AppendListEntry(n int) {
	d.AppendListEntryRaw(d.childVecs[0].pos, n)
}

func AppendRow1[T1 validTypes](ch *UDFDataChunk, v1 T1) {
	Append(&ch.Columns[0], v1)
}

func AppendRow2[T1, T2 validTypes](ch *UDFDataChunk, v1 T1, v2 T2) {
	Append(&ch.Columns[0], v1)
	Append(&ch.Columns[1], v2)
}

func AppendRow3[T1, T2, T3 validTypes](ch *UDFDataChunk, v1 T1, v2 T2, v3 T3) {
	Append(&ch.Columns[0], v1)
	Append(&ch.Columns[1], v2)
	Append(&ch.Columns[2], v3)
}

func Append[T validTypes | [32]float32](vec *Vector, v T) {
	arr := (*[1 << 31]T)(vec.data)
	arr[vec.pos] = v
	vec.pos++
}

func AppendMany[T validTypes](vec *Vector, v []T) {
	vec.pos += rawCopy(vec, v)
}

func rawCopy[T any](vec *Vector, v []T) int {
	return copy(VectorData[T](vec)[vec.pos:], v)
}

func (d *Vector) init(v C.duckdb_vector, writable bool) {
	logicalType := C.duckdb_vector_get_column_type(v)
	duckdbType := C.duckdb_get_type_id(logicalType)
	defer C.duckdb_destroy_logical_type(&logicalType)
	d.pos = 0
	d.listCapacity = 0
	d.vector = v
	d.data = C.duckdb_vector_get_data(d.vector)

	if writable {
		C.duckdb_vector_ensure_validity_writable(v)
		d.bitmask = C.duckdb_vector_get_validity(v)
	}

	switch duckdbType {
	case C.DUCKDB_TYPE_UNION:
		memberCount := int(C.duckdb_union_type_member_count(logicalType))
		if memberCount == 0 {
			panic("empty union")
		}
		d.childVecs = d.childVecs[:0]
		x := C.duckdb_struct_vector_get_child(d.vector, C.idx_t(0))
		da := C.duckdb_vector_get_data(x)
		fmt.Println((*[1 << 31]uint8)(da)[:10])

		for i := 0; i < memberCount+1; i++ {
			//if i < memberCount {
			//	memberType := C.duckdb_union_type_member_type(logicalType, C.idx_t(i))
			//	fmt.Println(i, memberType, C.duckdb_get_type_id(memberType))
			//	C.duckdb_destroy_logical_type(&memberType)
			//}

			v := AcquireVectorWr(x, writable)
			d.childVecs = append(d.childVecs, v)
		}

	case C.DUCKDB_TYPE_STRUCT:
		childCount := int(C.duckdb_struct_type_child_count(logicalType))
		d.childVecs = d.childVecs[:0]
		for i := 0; i < childCount; i++ {
			v := AcquireVectorWr(C.duckdb_struct_vector_get_child(d.vector, C.idx_t(i)), writable)
			d.childVecs = append(d.childVecs, v)
		}
	case C.DUCKDB_TYPE_LIST:
		v := AcquireVectorWr(C.duckdb_list_vector_get_child(d.vector), writable)
		d.childVecs = append(d.childVecs[:0], v)
	case C.DUCKDB_TYPE_ARRAY:
		v := AcquireVectorWr(C.duckdb_array_vector_get_child(d.vector), writable)
		d.childVecs = append(d.childVecs[:0], v)
	default:

	}
}

func AcquireVectorWr(v C.duckdb_vector, writeble bool) *Vector {
	vec := vectorPool.Get().(*Vector)
	vec.init(v, writeble)
	return vec
}

func AcquireVector(v C.duckdb_vector) *Vector {
	return AcquireVectorWr(v, true)
}

func ReleaseVector(v *Vector) {
	vectorPool.Put(v)
}

var vectorPool = sync.Pool{
	New: func() any {
		return &Vector{}
	},
}

func AcquireChunk(capacity int, chunk C.duckdb_data_chunk) *UDFDataChunk {
	cols := int(C.duckdb_data_chunk_get_column_count(chunk))
	c := chunkPool.Get().(*UDFDataChunk)
	c.chunk = chunk
	if cap(c.Columns) < cols {
		c.Columns = make([]Vector, cols)
	}
	c.Columns = c.Columns[:cols]
	c.Capacity = capacity
	for i := range c.Columns {
		c.Columns[i].init(C.duckdb_data_chunk_get_vector(chunk, C.uint64_t(i)), true)
	}
	return c
}

func ReleaseChunk(ch *UDFDataChunk) {
	for i := range ch.Columns {
		for j := range ch.Columns[i].childVecs {
			if ch.Columns[i].childVecs[j] != nil {
				ReleaseVector(ch.Columns[i].childVecs[j])
				ch.Columns[i].childVecs[j] = nil
			}
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
