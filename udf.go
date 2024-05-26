package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

void udf_bind(duckdb_bind_info info);
void udf_init(duckdb_init_info info);
void udf_local_init(duckdb_init_info info);
void udf_local_init_cleanup(duckdb_init_info info);
void udf_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837
void udf_destroy_data(void *);

typedef void (*init)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*bind)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19835

*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"github.com/google/uuid"
	"log"
	"reflect"
	"runtime/cgo"
	"sync"
	"unsafe"
)

type (
	ColumnDef struct {
		Name string
		V    any
	}
	Binding interface {
		Table() *Table
		InitScanner(vecSize int) Scanner
	}
	Table struct {
		Name             string
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
		Arguments() []any
		NamedArguments() map[string]any
		BindArguments(namedArgs map[string]any, args []any) (table Binding, err error)
	}
)

func udf_bind_error(info C.duckdb_bind_info, err error) {
	errstr := C.CString(err.Error())
	C.duckdb_bind_set_error(info, errstr)
	C.free(unsafe.Pointer(errstr))
}

//export udf_bind
func udf_bind(info C.duckdb_bind_info) {
	extra_info := C.duckdb_bind_get_extra_info(info)
	h := cgo.Handle(extra_info)
	tfunc := h.Value().(TableFunction)

	var args []any
	for i, v := range tfunc.Arguments() {
		typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		value := C.duckdb_bind_get_parameter(info, C.uint64_t(i))
		arg, err := getBindValue(typ, value)
		C.duckdb_destroy_value(&value)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		args = append(args, arg)
	}
	namedArgs := make(map[string]any)
	for name, v := range tfunc.NamedArguments() {
		typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		argName := C.CString(name)
		value := C.duckdb_bind_get_named_parameter(info, argName)
		C.free(unsafe.Pointer(argName))
		arg, err := getBindValue(typ, value)
		C.duckdb_destroy_value(&value)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		namedArgs[name] = arg
	}

	bind, err := tfunc.BindArguments(namedArgs, args)
	if err != nil {
		udf_bind_error(info, err)
		return
	}
	table := bind.Table()

	defer func() {
		if r := recover(); r != nil {
			log.Println(r, "PANIC")
		}
	}()

	var addCol = func(name string, typ C.duckdb_logical_type) {
		colName := C.CString(name)
		C.duckdb_bind_add_result_column(info, colName, typ)
		C.free(unsafe.Pointer(colName))
		C.free(unsafe.Pointer(typ))
	}

	for _, v := range table.Columns {
		if _, ok := v.V.([]string); ok {
			typ, err := getDuckdbTypeFromValue("")
			if err != nil {
				udf_bind_error(info, err)
				return
			}
			listTyp := C.duckdb_create_list_type(C.duckdb_create_logical_type(typ))
			addCol(v.Name, listTyp)
		} else if _, ok := v.V.([]uint32); ok {
			typ, err := getDuckdbTypeFromValue(uint32(0))
			if err != nil {
				udf_bind_error(info, err)
				return
			}
			listTyp := C.duckdb_create_list_type(C.duckdb_create_logical_type(typ))
			addCol(v.Name, listTyp)
		} else if enum, ok := v.V.(*Enum); ok {
			names := enum.Names()

			var alloc []byte
			var offsets = make([]int, 0, len(names))
			for i := range names {
				offsets = append(offsets, len(alloc))
				alloc = append(alloc, names[i]...)
				alloc = append(alloc, 0) // null-termination
			}
			if len(names) == 0 {
				offsets = append(offsets, len(alloc))
				alloc = append(alloc, 0) // null-termination
			}

			colName := unsafe.Pointer(C.CBytes(alloc))
			var ptrs = make([]unsafe.Pointer, len(offsets))
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

	handle := cgo.NewHandle(bind)

	C.duckdb_bind_set_cardinality(info, C.uint64_t(table.Cardinality), C.bool(table.ExactCardinality))
	C.duckdb_bind_set_bind_data(info, unsafe.Pointer(handle), C.duckdb_delete_callback_t(C.udf_destroy_data))
}

//export udf_destroy_data
func udf_destroy_data(data unsafe.Pointer) {
	h := cgo.Handle(data)
	h.Delete()
}

func malloc2(strs ...unsafe.Pointer) unsafe.Pointer {
	x := C.malloc(C.size_t(len(strs)) * C.size_t(8))
	for i, v := range strs {
		(*[1 << 31]unsafe.Pointer)(x)[i] = v
	}
	return x
}

//export udf_init
func udf_init(info C.duckdb_init_info) {
	count := int(C.duckdb_init_get_column_count(info))
	bind := getBind(info).Value().(Binding)
	table := bind.Table()

	table.Projection = make([]int, count)
	for i := 0; i < count; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, C.uint64_t(i)))
		table.Projection[i] = srcPos
	}
	C.duckdb_init_set_max_threads(info, C.uint64_t(table.MaxThreads))
}

//export udf_local_init_cleanup
func udf_local_init_cleanup(info C.duckdb_init_info) {
	h := cgo.Handle(info)
	h.Value().(Scanner).Close()
	h.Delete()
}

func getBind(info C.duckdb_init_info) cgo.Handle {
	return cgo.Handle(C.duckdb_init_get_bind_data(info))
}

func getScanner(info C.duckdb_function_info) cgo.Handle {
	return cgo.Handle(C.duckdb_function_get_local_init_data(info))
}

//export udf_local_init
func udf_local_init(info C.duckdb_init_info) {
	bind := getBind(info).Value().(Binding)
	vecSize := int(C.duckdb_vector_size())
	scanner := bind.InitScanner(vecSize)
	C.duckdb_init_set_init_data(info, unsafe.Pointer(cgo.NewHandle(scanner)), C.duckdb_delete_callback_t(C.udf_local_init_cleanup))
}

//export udf_callback
func udf_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	vecSize := int(C.duckdb_vector_size())
	colCount := int(C.duckdb_data_chunk_get_column_count(output))

	scanner := getScanner(info).Value().(Scanner)

	ch := acquireChunk(vecSize, colCount, output)
	size, err := scanner.Scan(ch)
	releaseChunk(ch)
	if err != nil {
		errstr := C.CString(err.Error())
		C.duckdb_function_set_error(info, errstr)
		C.free(unsafe.Pointer(errstr))
	} else {
		C.duckdb_data_chunk_set_size(output, C.uint64_t(size))
	}
}

type UDFOptions struct {
	ProjectionPushdown bool
}

func RegisterTableUDF(c *sql.Conn, name string, opts UDFOptions, function TableFunction) error {
	return c.Raw(func(driverConn any) error {
		conn, ok := driverConn.(driver.Conn)
		if !ok {
			return driver.ErrBadConn
		}
		return RegisterTableUDFConn(conn, name, opts, function)
	})
}

func RegisterTableUDFConn(c driver.Conn, _name string, opts UDFOptions, function TableFunction) error {
	duckConn := c.(*conn)
	name := C.CString(_name)
	defer C.free(unsafe.Pointer(name))

	handle := cgo.NewHandle(function)

	tableFunction := C.duckdb_create_table_function()
	C.duckdb_table_function_set_name(tableFunction, name)
	C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind))
	C.duckdb_table_function_set_init(tableFunction, C.init(C.udf_init))
	C.duckdb_table_function_set_local_init(tableFunction, C.init(C.udf_local_init))
	C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_callback))
	C.duckdb_table_function_supports_projection_pushdown(tableFunction, C.bool(opts.ProjectionPushdown))
	C.duckdb_table_function_set_extra_info(tableFunction, unsafe.Pointer(handle), C.duckdb_delete_callback_t(C.udf_destroy_data))

	for _, v := range function.Arguments() {
		argtype, err := getDuckdbTypeFromValue(v)
		if err != nil {
			return err
		}
		lt := C.duckdb_create_logical_type(argtype)
		C.duckdb_table_function_add_parameter(tableFunction, lt)
		C.duckdb_destroy_logical_type(&lt)
	}

	for name, v := range function.NamedArguments() {
		argtype, err := getDuckdbTypeFromValue(v)
		if err != nil {
			return err
		}
		lt := C.duckdb_create_logical_type(argtype)
		argName := C.CString(name)
		C.duckdb_table_function_add_named_parameter(tableFunction, argName, lt)

		C.duckdb_destroy_logical_type(&lt)
		C.free(unsafe.Pointer(argName))
	}

	state := C.duckdb_register_table_function(duckConn.duckdbCon, tableFunction)
	if state != 0 {
		return invalidTableFunctionError()
	}
	return nil
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

func getBindValue(t C.duckdb_type, v C.duckdb_value) (any, error) {
	if v == nil {
		return "", nil
	}
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
		if str == nil {
			return "", nil
		}
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
		c.Columns[i].init(vecSize, C.duckdb_data_chunk_get_vector(output, C.uint64_t(i)))
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

func (d *Vector) AppendBool(v bool) {
	d.bools[d.pos] = v
	d.pos++
}

var emptyString = []byte(" ")

func (d *Vector) appendBytes(v []byte) {
	sz := len(v)
	if sz == 0 {
		v = emptyString
		panic("fix empty string handling")
	}
	cstr := (*C.char)(unsafe.Pointer(&v[0]))
	C.duckdb_vector_assign_string_element_len(d.vector, C.uint64_t(d.pos), cstr, C.idx_t(sz))
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
