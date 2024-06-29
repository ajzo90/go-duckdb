package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"math/bits"
	"time"
	"unsafe"
)

type logicalTypesWrap struct {
	types []C.duckdb_logical_type
	ptr   unsafe.Pointer
}

func (l *logicalTypesWrap) free() {
	for i := range l.types {
		C.duckdb_destroy_logical_type(&l.types[i])
	}
	C.duckdb_free(l.ptr)
}

func (r *rows) prepLogicalTypes(columnCount int) logicalTypesWrap {
	ptr, types := mallocTypeSlice(columnCount)
	for i := 0; i < len(r.chunk.columnNames); i++ {
		types[i] = C.duckdb_column_logical_type(
			&r.res,
			C.idx_t(i),
		)
	}
	return logicalTypesWrap{types: types, ptr: ptr}
}

type logicalType struct {
}

type VectorI interface {
	Load(v *UDFDataChunk, idx int) error
	load(vec C.duckdb_vector, numValues int) error
	serialize(dst []byte, format string) []byte
	//Serialize([]byte) []byte
}

func initVec(lt C.duckdb_logical_type, numValues int) VectorI {
	duckdbType := C.duckdb_get_type_id(lt)
	switch duckdbType {
	// primitive types
	case C.DUCKDB_TYPE_BOOLEAN:
		return &Vec[bool]{}
	case C.DUCKDB_TYPE_UTINYINT:
		return &Vec[uint8]{}

		// "complex" type
	case C.DUCKDB_TYPE_ENUM:
		return &EnumType{}

	// composite types
	case C.DUCKDB_TYPE_STRUCT:
		return &StructType{}
	case C.DUCKDB_TYPE_LIST:
		childType := C.duckdb_list_type_child_type(lt)
		typ := C.duckdb_get_type_id(childType)
		defer C.duckdb_destroy_logical_type(&childType)
		switch typ {
		case C.DUCKDB_TYPE_BOOLEAN:
			return &ListType[bool]{}
		case C.DUCKDB_TYPE_UTINYINT:
			return &ListType[uint8]{}
		}
		panic(1)
	case C.DUCKDB_TYPE_ARRAY:
		childType := C.duckdb_list_type_child_type(lt)
		typ := C.duckdb_get_type_id(childType)
		defer C.duckdb_destroy_logical_type(&childType)
		switch typ {
		case C.DUCKDB_TYPE_BOOLEAN:
			return &ArrayType[bool]{}
		case C.DUCKDB_TYPE_UTINYINT:
			return &ArrayType[uint8]{}
		}
		panic(1)
	case C.DUCKDB_TYPE_UNION:
	case C.DUCKDB_TYPE_MAP:
		return &MapType{}
	}
	panic(1)
}

type MapType struct {
}

type StructType struct {
}

func (s *StructType) serialize(dst []byte, format string) []byte {
	//TODO implement me
	panic("implement me")
}

func createDataChunk(l *logicalTypesWrap) C.duckdb_data_chunk {
	return C.duckdb_create_data_chunk((*C.duckdb_logical_type)(l.ptr), C.idx_t(len(l.types)))
}

func (r *Rows) NextChunk(c *Chunk) error {
	c.Close()
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// load logical types
	//_ = r.prepLogicalTypes(len(r.chunk.columns))

	c.chunk = C.duckdb_stream_fetch_chunk(r.res)
	if c.chunk == nil {
		c.Close()
		return io.EOF
	}
	return nil
}

type Chunk = UDFDataChunk

type EnumType struct {
	get func(int) uint32
	sz  int
	m   [][]byte
}

func (e *EnumType) serialize(dst []byte, format string) []byte {
	//TODO implement me
	panic("implement me")
}

func (e *EnumType) Rows() int {
	return e.sz
}

func (e *EnumType) GetIndex(i int) uint32 {
	return e.get(i)
}

func (e *EnumType) GetBytes(i int) []byte {
	return e.m[e.get(i)]
}

type ArrayType[T validTypes] struct {
	elements []T
	arrSize  int
}

func (a *ArrayType[T]) serialize(dst []byte, format string) []byte {
	//TODO implement me
	panic("implement me")
}

func (a *ArrayType[T]) Rows() int {
	return len(a.elements) / a.arrSize
}

func (a *ArrayType[T]) GetRow(row int) []T {
	offset := row * a.arrSize
	return a.elements[offset:][:a.arrSize]
}

func (m *MapType) serialize(dst []byte, format string) []byte {
	return nil
}

func (m *MapType) load(vector C.duckdb_vector, numValues int) error {
	logical := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&logical)

	valueType := C.duckdb_map_type_value_type(logical)
	defer C.duckdb_destroy_logical_type(&valueType)

	keyType := C.duckdb_map_type_key_type(logical)
	defer C.duckdb_destroy_logical_type(&keyType)

	return nil
}

func (m *MapType) Load(ch *UDFDataChunk, idx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(idx))
	return m.load(vector, ch.NumValues())
}

func (s *StructType) load(vector C.duckdb_vector, numValues int) error {
	logical := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&logical)
	cnt := int(C.duckdb_struct_type_child_count(logical))

	for i := 0; i < cnt; i++ {
		name := C.duckdb_struct_type_child_name(logical, C.idx_t(i))
		typ := C.duckdb_struct_type_child_type(logical, C.idx_t(i))
		child := C.duckdb_struct_vector_get_child(vector, C.idx_t(i))

		C.duckdb_free(unsafe.Pointer(name))
		fmt.Println(C.GoString(name), typ, child)
	}

	panic("implement me")
}

func (s *StructType) Load(ch *UDFDataChunk, idx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(idx))
	return s.load(vector, ch.NumValues())
}

func (a *ArrayType[T]) load(vector C.duckdb_vector, numValues int) error {
	childVector := C.duckdb_list_vector_get_child(vector)
	childSz := int(C.duckdb_list_vector_get_size(vector))
	logical := C.duckdb_vector_get_column_type(vector)
	a.arrSize = int(C.duckdb_array_type_array_size(logical))
	C.duckdb_destroy_logical_type(&logical)
	var err error
	a.elements, err = getVector[T](DuckdbType[T](), childSz*a.arrSize, childVector)
	return err
}

func (a *ArrayType[T]) Load(ch *UDFDataChunk, colIdx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return a.load(vector, ch.NumValues())
}

func (l *ListType[T]) load(vector C.duckdb_vector, numValues int) error {
	childVector := C.duckdb_list_vector_get_child(vector)
	childSz := int(C.duckdb_list_vector_get_size(vector))

	childType := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&childType)
	//l.child = initVec(childType, childSz)

	var err error
	l.elements, err = getVector[T](DuckdbType[T](), childSz, childVector)
	l.list = castVec[duckdb_list_entry_t](vector)[:numValues]
	return err
}
func (l *ListType[T]) Load(ch *UDFDataChunk, colIdx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return l.load(vector, ch.NumValues())
}

func List[T validTypes](ch *UDFDataChunk, colIdx int) (*ListType[T], error) {
	var l = &ListType[T]{}
	return l, l.Load(ch, colIdx)
}

type ListType[T validTypes] struct {
	//child    VectorI
	list     []duckdb_list_entry_t
	elements []T
}

func (l *ListType[T]) serialize(dst []byte, format string) []byte {
	//TODO implement me
	panic("implement me")
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

func (ch *UDFDataChunk) Time(colIdx int) ([]Timestamp, error) {
	return genericGet[Timestamp](C.DUCKDB_TYPE_TIME, ch, colIdx)
}

func (ch *UDFDataChunk) Timestamp(colIdx int) ([]Timestamp, error) {
	return genericGet[Timestamp](C.DUCKDB_TYPE_TIMESTAMP, ch, colIdx)
}

func (ch *UDFDataChunk) Date(colIdx int) ([]Date, error) {
	return genericGet[Date](C.DUCKDB_TYPE_DATE, ch, colIdx)
}

func (ch *UDFDataChunk) Varchar(colIdx int) ([]Varchar, error) {
	return GetVector[Varchar](ch, colIdx)
}

func (ch *UDFDataChunk) UUID(colIdx int) ([]UUIDInternal, error) {
	return GetVector[UUIDInternal](ch, colIdx)
}

func (ch *UDFDataChunk) Close() {
	C.duckdb_destroy_data_chunk(&ch.chunk)
	ch.chunk = nil
}

func (ch *UDFDataChunk) NumValues() int {
	return int(C.duckdb_data_chunk_get_size(ch.chunk))
}

func (b HugeInt) Float64() int64 {
	return int64(b.lower)
}

func maskBlocks(n int) int {
	return (n-1)/64 + 1
}

func validity(vector C.duckdb_vector, n int) []uint64 {
	if n == 0 {
		return nil
	}
	validityX := C.duckdb_vector_get_validity(vector)
	if validityX == nil {
		return nil
	}
	return (*[1 << 31]uint64)(unsafe.Pointer(validityX))[:maskBlocks(n)]
}

func zeroValidity[T any](buf []T, validity []uint64) {
	ln := len(buf)
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

func genericGet[T validTypes](typ C.duckdb_type, ch *UDFDataChunk, colIdx int) ([]T, error) {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return getVector[T](typ, ch.NumValues(), vector)
}

func GetVector[T validTypes](ch *UDFDataChunk, colIdx int) ([]T, error) {
	return genericGet[T](DuckdbType[T](), ch, colIdx)
}

type Vec[T validTypes] struct {
	Data     []T
	Validity []uint64
}

func (v *Vec[T]) Load(ch *UDFDataChunk, colIdx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return v.load(vector, ch.NumValues())
}

func (v *Vec[T]) load(vector C.duckdb_vector, numValues int) error {
	return loadVector[T](v, DuckdbType[T](), numValues, vector)
}
func (v *Vec[T]) serialize(dst []byte, format string) []byte {
	return nil
}

func getVector[T validTypes](typ C.duckdb_type, n int, vector C.duckdb_vector) ([]T, error) {
	var vec = &Vec[T]{}
	err := loadVector[T](vec, typ, n, vector)
	return vec.Data, err
}

func __vec[T validTypes](v *Vec[T], n int, vector C.duckdb_vector) {
	v.Data = castVec[T](vector)[:n]
	v.Validity = validity(vector, n)
	zeroValidity(v.Data, v.Validity)
}

func loadVector[T validTypes](v *Vec[T], typ C.duckdb_type, n int, vector C.duckdb_vector) error {
	ty := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&ty)

	switch resTyp := C.duckdb_get_type_id(ty); resTyp {
	case typ:
		__vec(v, n, vector)
		return nil
	default:
		return fmt.Errorf("invalid typ in getVector %v %v", typ, resTyp)
	}
}

func (v *Varchar) Bytes() []byte {
	if v.length <= stringInlineLength {
		// inline data is stored from byte 4..16 (up to 12 bytes)
		return (*[1 << 31]byte)(unsafe.Pointer(&v.prefix))[:v.length:v.length]
	} else {
		// any longer strings are stored as a pointer in `ptr`
		return (*[1 << 31]byte)(unsafe.Pointer(v.ptr))[:v.length:v.length]
	}
}

func (d *Date) Date() time.Time {
	v := C.duckdb_from_date(C.duckdb_date(*d))
	return time.Date(int(v.year), time.Month(v.month), int(v.day), 0, 0, 0, 0, time.UTC)
}
func (d *Timestamp) Timestamp() time.Time {
	return time.UnixMicro(int64(d.micros)).UTC()
}
func (d *TimestampMilli) Timestamp() time.Time {
	return time.UnixMilli(int64(d.micros)).UTC()
}
func (d *TimestampSecond) Timestamp() time.Time {
	return time.Unix(int64(d.micros), 0).UTC()
}
func (d *TimestampNano) Timestamp() time.Time {
	return time.Unix(0, int64(d.micros)).UTC()
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

type DecimalType struct {
	scale uint8
	width uint8
	get   func(int) *big.Int
	rows  int
}

func (d *DecimalType) Rows() int {
	return d.rows
}

func (d *DecimalType) GetRow(idx int) Decimal {
	return Decimal{Width: d.width, Scale: d.scale, Value: d.get(idx)}
}

func loadDec[T int16 | int32 | int64 | HugeInt](d *DecimalType, vector C.duckdb_vector, n int, f func(T) *big.Int) {
	var v Vec[T]
	__vec(&v, n, vector)
	d.get = func(i int) *big.Int {
		return f(v.Data[i])
	}
}
func (d *EnumType) Load(ch *UDFDataChunk, colIdx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	return d.load(vector, ch.NumValues())
}

func (d *EnumType) load(vector C.duckdb_vector, numValue int) error {

	logical := C.duckdb_vector_get_column_type(vector)
	dictSz := C.duckdb_enum_dictionary_size(logical)
	d.m = make([][]byte, dictSz)
	for i := 0; i < int(dictSz); i++ {
		v := C.duckdb_enum_dictionary_value(logical, C.idx_t(i))
		d.m[i] = []byte(C.GoString(v))
		C.duckdb_free(unsafe.Pointer(v))
	}

	phys := C.duckdb_enum_internal_type(logical)
	C.duckdb_destroy_logical_type(&logical)
	d.sz = numValue
	switch phys {
	default:
		return fmt.Errorf("invalid enum type %v", phys)
	case C.DUCKDB_TYPE_UTINYINT:
		var v Vec[uint8]
		__vec(&v, numValue, vector)
		d.get = func(i int) uint32 {
			return uint32(v.Data[i])
		}
	case C.DUCKDB_TYPE_USMALLINT:
		var v Vec[uint16]
		__vec(&v, numValue, vector)
		d.get = func(i int) uint32 {
			return uint32(v.Data[i])
		}
	case C.DUCKDB_TYPE_UINTEGER:
		var v Vec[uint32]
		__vec(&v, numValue, vector)
		d.get = func(i int) uint32 {
			return uint32(v.Data[i])
		}

	}
	return nil
}
func (d *DecimalType) Load(ch *UDFDataChunk, colIdx int) error {
	vector := C.duckdb_data_chunk_get_vector(ch.chunk, C.idx_t(colIdx))
	logical := C.duckdb_vector_get_column_type(vector)
	phys := C.duckdb_decimal_internal_type(logical)
	d.scale = uint8(C.duckdb_decimal_scale(logical))
	d.width = uint8(C.duckdb_decimal_width(logical))
	C.duckdb_destroy_logical_type(&logical)
	d.rows = ch.NumValues()

	switch phys {
	case C.DUCKDB_TYPE_SMALLINT:
		loadDec(d, vector, ch.NumValues(), func(i int16) *big.Int {
			return big.NewInt(int64(i))
		})
	case C.DUCKDB_TYPE_INTEGER:
		loadDec(d, vector, ch.NumValues(), func(i int32) *big.Int {
			return big.NewInt(int64(i))
		})
	case C.DUCKDB_TYPE_BIGINT:
		loadDec(d, vector, ch.NumValues(), func(i int64) *big.Int {
			return big.NewInt(i)
		})
	case C.DUCKDB_TYPE_HUGEINT:
		//loadDec(d, vector, ch.NumValues(), func(v HugeInt) *big.Int {
		//	return hugeIntToNative(C.duckdb_hugeint{
		//		lower: v.lower,
		//		upper: v.upper,
		//	})
		//})
	default:
		return fmt.Errorf("invalid decimal type %v", phys)
	}
	return nil
}

type primitiveTypes interface {
	bool | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

type validTypes interface {
	primitiveTypes |
		Varchar | Date |
		Timestamp | TimestampMilli | TimestampNano | TimestampSecond |
		UUIDInternal | IntervalInternal |
		HugeInt | Time | TimeTZ |
		C.duckdb_list_entry
}

func (i IntervalInternal) Interval() Interval {
	return Interval{
		Days:   int32(i.days),
		Months: int32(i.months),
		Micros: int64(i.micros),
	}
}

type (
	Varchar          duckdb_string_t
	HugeInt          C.duckdb_hugeint
	Timestamp        C.duckdb_timestamp
	TimestampSecond  C.duckdb_timestamp
	TimestampMilli   C.duckdb_timestamp
	TimestampNano    C.duckdb_timestamp
	TimestampTz      Timestamp
	Date             C.duckdb_date
	Time             C.duckdb_time
	TimeTZ           C.duckdb_time_tz
	UUIDInternal     C.duckdb_hugeint
	IntervalInternal C.duckdb_interval
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
