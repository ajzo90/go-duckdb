package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

void udf_bind(duckdb_bind_info info);

void udf_init(duckdb_init_info info);
void udf_init_cleanup(duckdb_init_info info);

void udf_local_init(duckdb_init_info info);
void udf_local_init_cleanup(duckdb_init_info info);

void udf_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837

typedef void (*init)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*bind)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19835
*/
import "C"

import (
	"database/sql"
	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"reflect"
	"sync"
	"unsafe"
)

type (
	Ref       int64
	ColumnDef struct {
		Name string
		V    any
	}
	Schema struct {
		Columns          []ColumnDef
		Projection       []int
		MaxThreads       int
		Cardinality      int
		ExactCardinality bool
	}
	Scanner interface {
		Scan(chunk *DataChunk) (int, error)
		Close()
	}
	TableFunction interface {
		GetArguments() []any
		BindArguments(args ...any) (schema Ref)
		GetSchema(schema Ref) *Schema
		DestroySchema(schema Ref)
		InitScanner(ref Ref, vecSize int) (scanner Ref)
		GetScanner(scanner Ref) Scanner
	}
)

var tableFuncs = make([]TableFunction, 0, 1024)

func udf_bind_error(info C.duckdb_bind_info, err error) {
	errstr := C.CString(err.Error())
	C.duckdb_bind_set_error(info, errstr)
	C.free(unsafe.Pointer(errstr))
}

//export udf_bind
func udf_bind(info C.duckdb_bind_info) {
	extra_info := C.duckdb_bind_get_extra_info(info)
	tfunc := tableFuncs[*(*int)(extra_info)]

	var args []any
	for i, v := range tfunc.GetArguments() {
		typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		value := C.duckdb_bind_get_parameter(info, C.ulonglong(i))
		arg, err := getValue(typ, value)
		C.duckdb_destroy_value(&value)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		args = append(args, arg)
	}

	schemaRef := tfunc.BindArguments(args...)
	schema := tfunc.GetSchema(schemaRef)

	var addCol = func(name string, typ C.duckdb_logical_type) {
		colName := C.CString(name)
		C.duckdb_bind_add_result_column(info, colName, typ)
		C.free(unsafe.Pointer(colName))
	}

	for _, v := range schema.Columns {
		if _, ok := v.V.([]string); ok {
			typ, err := getDuckdbTypeFromValue("")
			if err != nil {
				udf_bind_error(info, err)
				return
			}
			listTyp := C.duckdb_create_list_type(C.duckdb_create_logical_type(typ))
			addCol(v.Name, listTyp)
		} else if enum, ok := v.V.(*Enum); ok {
			names := enum.Names()
			var ptrs = make([]unsafe.Pointer, len(names))
			var alloc []byte
			var offsets = make([]int, 0, len(names))
			for i := range names {
				offsets = append(offsets, len(alloc))
				alloc = append(alloc, names[i]...)
				alloc = append(alloc, 0) // null-termination
			}

			colName := unsafe.Pointer(C.CBytes(alloc))
			for i := range offsets {
				ptrs[i] = unsafe.Add(colName, offsets[i])
			}
			p := (**C.char)(malloc2(ptrs...))
			typ := C.duckdb_create_enum_type(p, C.idx_t(len(ptrs)))
			addCol(v.Name, typ)
			C.free(colName)
			C.free(unsafe.Pointer(p))
		} else {
			typ, err := getDuckdbTypeFromValue(v.V)
			if err != nil {
				udf_bind_error(info, err)
				return
			}
			addCol(v.Name, C.duckdb_create_logical_type(typ))
		}
	}

	C.duckdb_bind_set_cardinality(info, C.ulonglong(schema.Cardinality), C.bool(schema.ExactCardinality))
	C.duckdb_bind_set_bind_data(info, malloc(int(schemaRef)), C.duckdb_delete_callback_t(C.free))
}

func malloc2(strs ...unsafe.Pointer) unsafe.Pointer {
	x := C.malloc(C.ulong(len(strs)) * C.ulong(8))
	for i, v := range strs {
		(*[1 << 31]unsafe.Pointer)(x)[i] = v
	}
	return x
}

//export udf_init_cleanup
func udf_init_cleanup(info C.duckdb_init_info) {
	refs := *(*[2]int)(info)
	schemaRef, tblRef := refs[0], refs[1]
	tfunc := tableFuncs[tblRef]
	tfunc.DestroySchema(Ref(schemaRef))
	C.free(unsafe.Pointer(info))
	//log.Println("udf_init_cleanup", schemaRef, tblRef)
}

//export udf_init
func udf_init(info C.duckdb_init_info) {
	count := int(C.duckdb_init_get_column_count(info))
	udfRef := *(*int)(C.duckdb_init_get_extra_info(info))
	tfunc := tableFuncs[udfRef]
	schemaRef := *(*Ref)(C.duckdb_init_get_bind_data(info))
	schema := tfunc.GetSchema(schemaRef)
	schema.Projection = make([]int, count)
	for i := 0; i < count; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, C.ulonglong(i)))
		schema.Projection[i] = srcPos
	}
	C.duckdb_init_set_max_threads(info, C.ulonglong(schema.MaxThreads))
	C.duckdb_init_set_init_data(info, malloc(int(schemaRef), int(udfRef)), C.duckdb_delete_callback_t(C.udf_init_cleanup))
}

//export udf_local_init_cleanup
func udf_local_init_cleanup(info C.duckdb_init_info) {
	refs := *(*[2]int)(info)
	scanRef, tblRef := refs[0], refs[1]
	tblFunc := tableFuncs[tblRef]
	tblFunc.GetScanner(Ref(scanRef)).Close()
	C.free(unsafe.Pointer(info))
	//log.Println("udf_local_init_cleanup", scanRef, tblRef)
}

//export udf_local_init
func udf_local_init(info C.duckdb_init_info) {
	udfRef := *(*int)(C.duckdb_init_get_extra_info(info))
	tfunc := tableFuncs[udfRef]
	schema := *(*Ref)(C.duckdb_init_get_bind_data(info))
	vecSize := int(C.duckdb_vector_size())
	scanRef := int(tfunc.InitScanner(schema, vecSize))
	C.duckdb_init_set_init_data(info, malloc(scanRef, udfRef), C.duckdb_delete_callback_t(C.udf_local_init_cleanup))
}

//export udf_callback
func udf_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	extraInfo := C.duckdb_function_get_extra_info(info)
	tblFunc := tableFuncs[*(*int)(extraInfo)]
	vecSize := int(C.duckdb_vector_size())
	colCount := int(C.duckdb_data_chunk_get_column_count(output))
	local := *(*Ref)(C.duckdb_function_get_local_init_data(info))
	ch := acquireChunk(vecSize, colCount, output)
	size, err := tblFunc.GetScanner(local).Scan(ch)
	releaseChunk(ch)
	if err != nil {
		errstr := C.CString(err.Error())
		C.duckdb_function_set_error(info, errstr)
		C.free(unsafe.Pointer(errstr))
	} else {
		C.duckdb_data_chunk_set_size(output, C.ulonglong(size))
	}
}

type UDFOptions struct {
	ProjectionPushdown bool
}

func RegisterTableUDF(c *sql.Conn, name string, opts UDFOptions, function TableFunction) error {
	return c.Raw(func(dconn any) error {
		duckConn := dconn.(*conn)
		name := C.CString(name)
		defer C.free(unsafe.Pointer(name))

		extra_info := malloc(len(tableFuncs))
		tableFuncs = append(tableFuncs, function)

		tableFunction := C.duckdb_create_table_function()
		C.duckdb_table_function_set_name(tableFunction, name)
		C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind))
		C.duckdb_table_function_set_init(tableFunction, C.init(C.udf_init))
		C.duckdb_table_function_set_local_init(tableFunction, C.init(C.udf_local_init))
		C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_callback))
		C.duckdb_table_function_supports_projection_pushdown(tableFunction, C.bool(opts.ProjectionPushdown))
		C.duckdb_table_function_set_extra_info(tableFunction, extra_info, C.duckdb_delete_callback_t(C.free))

		for _, v := range function.GetArguments() {
			argtype, err := getDuckdbTypeFromValue(v)
			if err != nil {
				return err
			}
			C.duckdb_table_function_add_parameter(tableFunction, C.duckdb_create_logical_type(argtype))
		}

		state := C.duckdb_register_table_function(duckConn.duckdbCon, tableFunction)
		if state != 0 {
			return invalidTableFunctionError()
		}
		return nil
	})
}

type Enum struct {
	values []string
	m      map[uint64]uint32
	mtx    sync.RWMutex
}

func NewEnum() *Enum {
	return &Enum{
		values: make([]string, 0, 1024),
		m:      make(map[uint64]uint32),
	}
}

func (enum *Enum) Names() []string {
	enum.mtx.RLock()
	defer enum.mtx.RUnlock()
	return enum.values
}

func (enum *Enum) add(x uint64, s []byte) uint32 {
	enum.mtx.Lock()
	defer enum.mtx.Unlock()

	if id, ok := enum.m[x]; ok {
		return id
	}

	id := uint32(len(enum.values))
	v := string(s)
	enum.values = append(enum.values, v)
	enum.m[x] = id
	return id
}

func (enum *Enum) Register(b []byte) uint32 {
	x := xxhash.Sum64(b)
	enum.mtx.RLock()
	id, ok := enum.m[x]
	enum.mtx.RUnlock()
	if ok {
		return id
	}
	return enum.add(x, b)
}

func getDuckdbTypeFromValue(v any) (C.duckdb_type, error) {
	switch v.(type) {
	case uuid.UUID:
		return C.DUCKDB_TYPE_UUID, nil
	case uint64:
		return C.DUCKDB_TYPE_UBIGINT, nil
	case int32:
		return C.DUCKDB_TYPE_INTEGER, nil
	case int16:
		return C.DUCKDB_TYPE_SMALLINT, nil
	case int8:
		return C.DUCKDB_TYPE_TINYINT, nil
	case int64:
		return C.DUCKDB_TYPE_BIGINT, nil
	case uint32:
		return C.DUCKDB_TYPE_UINTEGER, nil
	case uint16:
		return C.DUCKDB_TYPE_USMALLINT, nil
	case uint8:
		return C.DUCKDB_TYPE_UTINYINT, nil
	case string:
		return C.DUCKDB_TYPE_VARCHAR, nil
	case bool:
		return C.DUCKDB_TYPE_BOOLEAN, nil
	case float64:
		return C.DUCKDB_TYPE_DOUBLE, nil
	case float32:
		return C.DUCKDB_TYPE_FLOAT, nil
	default:
		return C.DUCKDB_TYPE_INVALID, unsupportedTypeError(reflect.TypeOf(v).String())
	}
}

func getValue(t C.duckdb_type, v C.duckdb_value) (any, error) {
	switch t {
	case C.DUCKDB_TYPE_BOOLEAN:
		if C.duckdb_get_int64(v) != 0 {
			return true, nil
		} else {
			return false, nil
		}
	case C.DUCKDB_TYPE_BIGINT:
		return int64(C.duckdb_get_int64(v)), nil
	case C.DUCKDB_TYPE_DOUBLE:
		panic("not implemented")
	case C.DUCKDB_TYPE_VARCHAR:
		str := C.duckdb_get_varchar(v)
		ret := C.GoString(str)
		C.duckdb_free(unsafe.Pointer(str))
		return ret, nil
	default:
		return nil, unsupportedTypeError(reflect.TypeOf(v).String())
	}
}

var chunkPool = sync.Pool{
	New: func() any {
		return &DataChunk{}
	},
}

func acquireChunk(vecSize int, cols int, output C.duckdb_data_chunk) *DataChunk {
	c := chunkPool.Get().(*DataChunk)
	if cap(c.Columns) < cols {
		c.Columns = make([]Vector, cols)
	}
	c.Columns = c.Columns[:cols]
	for i := range c.Columns {
		c.Columns[i].init(vecSize, C.duckdb_data_chunk_get_vector(output, C.ulonglong(i)))
	}
	return c
}

func releaseChunk(ch *DataChunk) {
	for i := range ch.Columns {
		if ch.Columns[i].childVec != nil {
			releaseVector(ch.Columns[i].childVec)
			ch.Columns[i].childVec = nil
		}
	}
	chunkPool.Put(ch)
}

type DataChunk struct {
	Columns []Vector
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
	bools       []bool
	uuids       []C.duckdb_hugeint
	listEntries []C.duckdb_list_entry
	bitmask     *C.uint64_t
}

func (d *Vector) AppendUInt64(v uint64) {
	if d == nil {
		return
	}
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
	d.ReserveListSize(d.childVec.pos + n) // todo: call this only once for [][]T or []T
}

func (d *Vector) Child() *Vector {
	return d.childVec
}

// for reference
func (d *Vector) AppendStringList(v [][]byte) {
	d.AppendListEntry(len(v))
	for _, v := range v {
		d.childVec.appendBytes(v)
	}
}

// for reference
func (d *Vector) AppendStringLists(v [][][]byte) {
	for _, v := range v {
		d.AppendListEntry(len(v))
		for _, v := range v {
			d.childVec.appendBytes(v)
		}
	}
}

func AppendUint32(vec *Vector, v float64) {
	vec.AppendUInt32(uint32(v))
}

func AppendFloat64(vec *Vector, v float64) {
	vec.AppendFloat64(v)
}

func AppendBytes(vec *Vector, v []byte) {
	vec.AppendBytes(v)
}

func AppendUUID(vec *Vector, v []byte) {
	if vec == nil {
		return
	}
	vec.uuids[vec.pos] = uuidToHugeInt(UUID(v))
	vec.pos++
}

func RawCopy(vec *Vector, v []byte) {
	if vec == nil {
		return
	}
	copy((*[1 << 31]byte)(vec.ptr)[:], v)
	vec.pos += len(v) / 16
}

func (d *Vector) AppendUInt32(v uint32) {
	if d == nil {
		return
	}
	d.uint32s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendUInt16(v uint16) {
	if d == nil {
		return
	}
	d.uint16s[d.pos] = v
	d.pos++
}

func (d *Vector) GetSize() int {
	return d.pos
}

func (d *Vector) SetSize(n int) {
	if d == nil {
		return
	}
	for d.pos < n {
		C.duckdb_validity_set_row_invalid(d.bitmask, C.ulonglong(d.pos))
		d.pos++
	}
}

func (d *Vector) AppendUInt8(v uint8) {
	if d == nil {
		return
	}
	d.uint8s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendFloat64(v float64) {
	if d == nil {
		return
	}
	d.float64s[d.pos] = v
	d.pos++
}

func (d *Vector) AppendBool(v bool) {
	if d == nil {
		return
	}
	d.bools[d.pos] = v
	d.pos++
}

func (d *Vector) appendBytes(v []byte) {
	cstr := (*C.char)(unsafe.Pointer(&v[0]))
	C.duckdb_vector_assign_string_element_len(d.vector, C.ulonglong(d.pos), cstr, C.idx_t(len(v)))
	d.pos++
}

func (d *Vector) AppendBytes(v []byte) {
	if d == nil {
		return
	}
	d.appendBytes(v)
}

func (d *Vector) AppendNull() {
	if d == nil {
		return
	}
	C.duckdb_validity_set_row_invalid(d.bitmask, C.ulonglong(d.pos))
	d.pos++
}

func initVecSlice[T uint64 | uint32 | uint16 | uint8 | float64 | float32 | bool | [16]uint8 | C.duckdb_hugeint | C.duckdb_list_entry](sl *[]T, ptr unsafe.Pointer, sz int) {
	*sl = (*[1 << 31]T)(ptr)[:sz]
}

func (d *Vector) init(sz int, v C.duckdb_vector) {
	logicalType := C.duckdb_vector_get_column_type(v)
	duckdbType := C.duckdb_get_type_id(logicalType)
	d.pos = 0
	d.vector = v
	d.ptr = C.duckdb_vector_get_data(v)
	initVecSlice(&d.uint64s, d.ptr, sz)
	initVecSlice(&d.uint32s, d.ptr, sz)
	initVecSlice(&d.uint16s, d.ptr, sz)
	initVecSlice(&d.uint8s, d.ptr, sz)
	initVecSlice(&d.float64s, d.ptr, sz)
	initVecSlice(&d.bools, d.ptr, sz)
	initVecSlice(&d.uuids, d.ptr, sz)
	initVecSlice(&d.listEntries, d.ptr, sz)

	C.duckdb_vector_ensure_validity_writable(v)
	d.bitmask = C.duckdb_vector_get_validity(v)

	if duckdbType == C.DUCKDB_TYPE_LIST {
		d.childVec = acquireVector()
		d.childVec.init(sz, C.duckdb_list_vector_get_child(d.vector))
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

func malloc(v ...int) unsafe.Pointer {
	extra_info := C.malloc(C.ulong(len(v)) * C.ulong(1*unsafe.Sizeof(int(0))))
	for i, v := range v {
		(*[1024]int)(extra_info)[i] = v
	}
	return extra_info
}
