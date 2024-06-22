package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"time"
	"unsafe"
)

func (r *rows) NextChunk(c *Chunk) error {
	c.Close()
	r.mtx.Lock()
	defer r.mtx.Unlock()

	c.chunk = C.duckdb_stream_fetch_chunk(r.res)
	if c.chunk == nil {
		c.Close()
		return io.EOF
	}
	return nil
}

type Chunk = UDFDataChunk

func List[T validTypes](ch *UDFDataChunk, colIdx int) (*ListType[T], error) {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	childVector := C.duckdb_list_vector_get_child(vector)
	entries := castVec[duckdb_list_entry_t](vector)[:ch.NumValues()]
	childSz := int(C.duckdb_list_vector_get_size(vector))
	elements, err := getVector[T](DuckdbType[T](), childSz, childVector)
	if err != nil {
		return nil, err
	}

	return &ListType[T]{list: entries, elements: elements}, nil
}

type ListType[T validTypes] struct {
	list     []duckdb_list_entry_t
	elements []T
}

func (l *ListType[T]) Rows() int {
	return len(l.list)
}

func (l *ListType[T]) GetRow(row int) []T {
	entry := l.list[row]
	return l.elements[entry.offset : entry.offset+entry.length]
}

func (ch *UDFDataChunk) Uint16(colIdx int) ([]uint16, error) {
	return GetVector[uint16](ch, colIdx)
}

func (ch *UDFDataChunk) Int8(colIdx int) ([]int8, error) {
	return GetVector[int8](ch, colIdx)
}

func (ch *UDFDataChunk) Int16(colIdx int) ([]int16, error) {
	return GetVector[int16](ch, colIdx)
}

func (ch *UDFDataChunk) Uint64(colIdx int) ([]uint64, error) {
	return GetVector[uint64](ch, colIdx)
}

func (ch *UDFDataChunk) Uint32(colIdx int) ([]uint32, error) {
	return GetVector[uint32](ch, colIdx)
}

func (ch *UDFDataChunk) Int32(colIdx int) ([]int32, error) {
	return GetVector[int32](ch, colIdx)
}

func (ch *UDFDataChunk) Uint8(colIdx int) ([]uint8, error) {
	return GetVector[uint8](ch, colIdx)
}

func (ch *UDFDataChunk) Bool(colIdx int) ([]bool, error) {
	return GetVector[bool](ch, colIdx)
}

func (ch *UDFDataChunk) Int64(colIdx int) ([]int64, error) {
	return GetVector[int64](ch, colIdx)
}

func (ch *UDFDataChunk) Float32(colIdx int) ([]float32, error) {
	return GetVector[float32](ch, colIdx)
}

func (ch *UDFDataChunk) Float64(colIdx int) ([]float64, error) {
	return GetVector[float64](ch, colIdx)
}

func (ch *UDFDataChunk) VarcharList(colIdx int) (*ListType[Varchar], error) {
	return List[Varchar](ch, colIdx)
}

func (ch *UDFDataChunk) Uint32List(colIdx int) (*ListType[uint32], error) {
	return List[uint32](ch, colIdx)
}

func (ch *UDFDataChunk) Time(colIdx int) ([]DateTime, error) {
	return genericGet[DateTime](C.DUCKDB_TYPE_TIME, ch, colIdx)
}

func (ch *UDFDataChunk) Timestamp(colIdx int) ([]DateTime, error) {
	return genericGet[DateTime](C.DUCKDB_TYPE_TIMESTAMP, ch, colIdx)
}

func (ch *UDFDataChunk) Date(colIdx int) ([]Date, error) {
	return genericGet[C.duckdb_date](C.DUCKDB_TYPE_DATE, ch, colIdx)
}

func (ch *UDFDataChunk) Varchar(colIdx int) ([]Varchar, error) {
	return GetVector[Varchar](ch, colIdx)
}

func (ch *UDFDataChunk) Close() {
	C.duckdb_destroy_data_chunk(&ch.chunk)
	ch.chunk = nil
}

func (ch *UDFDataChunk) NumValues() int {
	return int(C.duckdb_data_chunk_get_size(ch.chunk))
}

func (b BigInt) Float64() int64 {
	return int64(b.lower)
}

func maskBlocks(n int) int {
	return (n-1)/64 + 1
}

func clearFromMask[T any](buf []T, vector C.duckdb_vector) {
	ln := len(buf)
	validityX := C.duckdb_vector_get_validity(vector)
	if validityX == nil {
		return
	}
	if ln == 0 {
		return
	}
	var validity = (*[1 << 31]uint64)(unsafe.Pointer(validityX))[:maskBlocks(ln)]
	var z T
	for k, bitset := range validity {
		bitset = ^bitset
		if k == ln>>6 {
			bitset &= (1 << uint(ln&63)) - 1
		}

		for bitset != 0 {
			idx := k<<6 + bits.TrailingZeros64(bitset)
			buf[idx] = z
			bitset ^= bitset & -bitset
		}
	}
}

func castVec[T any](vector C.duckdb_vector) *[1 << 31]T {
	ptr := C.duckdb_vector_get_data(vector)
	return (*[1 << 31]T)(ptr)
}

func (u UUIDInternal) UUID() UUID {
	var uuid [16]byte
	// We need to flip the sign bit of the signed hugeint to transform it to UUID bytes
	binary.BigEndian.PutUint64(uuid[:8], uint64(u.upper)^1<<63)
	binary.BigEndian.PutUint64(uuid[8:], uint64(u.lower))
	return uuid
}

func (ch *UDFDataChunk) UUID(colIdx int) ([]UUIDInternal, error) {
	return GetVector[UUIDInternal](ch, colIdx)
}

func genericGet[T validTypes](typ C.duckdb_type, ch *UDFDataChunk, colIdx int) ([]T, error) {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return getVector[T](typ, ch.NumValues(), vector)
}

func GetVector[T validTypes](ch *UDFDataChunk, colIdx int) ([]T, error) {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return getVector[T](DuckdbType[T](), ch.NumValues(), vector)
}

func getVector[T validTypes](typ C.duckdb_type, n int, vector C.duckdb_vector) ([]T, error) {
	ty := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&ty)

	switch resTyp := C.duckdb_get_type_id(ty); resTyp {
	case typ:
		arr := castVec[T](vector)[:n]
		clearFromMask(arr, vector)
		return arr, nil
	default:
		return nil, fmt.Errorf("invalid typ in getVector %v %v", typ, resTyp)
	}
}

func (v Varchar) Varchar() string {
	return string(v.Bytes())
}

func (v Varchar) Bytes() []byte {
	if v.length <= stringInlineLength {
		// inline data is stored from byte 4..16 (up to 12 bytes)
		return (*[1 << 31]byte)(unsafe.Pointer(&v.prefix))[:v.length:v.length]
	} else {
		// any longer strings are stored as a pointer in `ptr`
		return (*[1 << 31]byte)(unsafe.Pointer(v.ptr))[:v.length:v.length]
	}
}

func (d Date) Date() time.Time {
	v := C.duckdb_from_date(d)
	return time.Date(int(v.year), time.Month(v.month), int(v.day), 0, 0, 0, 0, time.UTC)
}
func (d DateTime) Timestamp() time.Time {
	return time.UnixMicro(int64(d.micros)).UTC()
}

func DuckdbType[T any]() C.duckdb_type {
	var v T
	var x any = v

	switch x.(type) {
	default:
		return C.DUCKDB_TYPE_INVALID
	case bool:
		return C.DUCKDB_TYPE_BOOLEAN
	case int8:
		return C.DUCKDB_TYPE_TINYINT
	case int16:
		return C.DUCKDB_TYPE_SMALLINT
	case int32:
		return C.DUCKDB_TYPE_INTEGER
	case int64:
		return C.DUCKDB_TYPE_BIGINT
	case uint8:
		return C.DUCKDB_TYPE_UTINYINT
	case uint16:
		return C.DUCKDB_TYPE_USMALLINT
	case uint32:
		return C.DUCKDB_TYPE_UINTEGER
	case uint64:
		return C.DUCKDB_TYPE_UBIGINT
	case float32:
		return C.DUCKDB_TYPE_FLOAT
	case float64:
		return C.DUCKDB_TYPE_DOUBLE
	case Varchar:
		return C.DUCKDB_TYPE_VARCHAR
	case UUIDInternal:
		return C.DUCKDB_TYPE_UUID
	}
}

type validTypes interface {
	bool | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64 |
		Varchar | UUIDInternal | C.duckdb_timestamp | C.duckdb_date | C.duckdb_time | C.duckdb_time_tz | DateTime | C.duckdb_hugeint | C.duckdb_list_entry
}

type (
	Varchar      = duckdb_string_t
	UUIDInternal C.duckdb_hugeint
	Date         = C.duckdb_date
	DateTime     C.duckdb_timestamp
	BigInt       = C.duckdb_hugeint
)

//func (ch *UDFDataChunk) Enum(colIdx int) ([]any, error) {
//	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
//	columnType := C.duckdb_vector_get_column_type(vector)
//
//	var idx uint64
//	internalType := C.duckdb_enum_internal_type(columnType)
//	switch internalType {
//	case C.DUCKDB_TYPE_UTINYINT:
//		idx = uint64(get[uint8](vector, rowIdx))
//	case C.DUCKDB_TYPE_USMALLINT:
//		idx = uint64(get[uint16](vector, rowIdx))
//	case C.DUCKDB_TYPE_UINTEGER:
//		idx = uint64(get[uint32](vector, rowIdx))
//	default:
//		return nil, errInvalidType
//	}
//
//	val := C.duckdb_enum_dictionary_value(columnType, (C.idx_t)(idx))
//	defer C.duckdb_free(unsafe.Pointer(val))
//	return C.GoVarchar(val), nil
//
//}

//var timeConverter = map[C.duckdb_type]func(int64) time.Time{
//	C.DUCKDB_TYPE_TIME: func(i int64) time.Time {
//		return time.UnixMicro(i).UTC()
//	},
//	C.DUCKDB_TYPE_TIME_TZ: func(i int64) time.Time {
//		panic("not implemented")
//	},
//	C.DUCKDB_TYPE_TIMESTAMP_TZ: func(micros int64) time.Time {
//		return time.UnixMicro(micros).UTC()
//	},
//	C.DUCKDB_TYPE_TIMESTAMP: func(micros int64) time.Time {
//		return time.UnixMicro(micros).UTC()
//	},
//	C.DUCKDB_TYPE_TIMESTAMP_S: func(i int64) time.Time {
//		return time.Unix(i, 0).UTC()
//	},
//	C.DUCKDB_TYPE_TIMESTAMP_MS: func(i int64) time.Time {
//		return time.Unix(i/1000, (i%1000)*1000).UTC()
//	},
//	C.DUCKDB_TYPE_TIMESTAMP_NS: func(i int64) time.Time {
//		return time.Unix(0, i).UTC()
//	},
//}
