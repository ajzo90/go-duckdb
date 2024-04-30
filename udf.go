package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

void udf_bind(duckdb_bind_info info);

void udf_init(duckdb_init_info info);

void udf_local_init(duckdb_init_info info);

void udf_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837

typedef void (*init)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*bind)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19835
*/
import "C"

import (
	"database/sql"
	"github.com/google/uuid"
	"log"
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
	}
	TableFunction interface {
		GetArguments() []any
		BindArguments(args ...any) (schema Ref)
		GetSchema(schema Ref) *Schema
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
	for _, v := range schema.Columns {
		t, err := getDuckdbTypeFromValue(v.V)
		if err != nil {
			udf_bind_error(info, err)
			return
		}

		colName := C.CString(v.Name)
		C.duckdb_bind_add_result_column(info, colName, C.duckdb_create_logical_type(t))
		C.free(unsafe.Pointer(colName))
	}

	C.duckdb_bind_set_cardinality(info, C.ulonglong(schema.Cardinality), C.bool(schema.ExactCardinality))
	C.duckdb_bind_set_bind_data(info, malloc(int(schemaRef)), C.duckdb_delete_callback_t(C.free))
}

//export udf_init
func udf_init(info C.duckdb_init_info) {
	count := int(C.duckdb_init_get_column_count(info))
	tfunc := tableFuncs[*(*int)(C.duckdb_init_get_extra_info(info))]
	schema := tfunc.GetSchema(*(*Ref)(C.duckdb_init_get_bind_data(info)))
	schema.Projection = make([]int, count)
	for i := 0; i < count; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, C.ulonglong(i)))
		schema.Projection[i] = srcPos
	}
	C.duckdb_init_set_max_threads(info, C.ulonglong(schema.MaxThreads))
}

var x int64

//export udf_local_init
func udf_local_init(info C.duckdb_init_info) {
	tfunc := tableFuncs[*(*int)(C.duckdb_init_get_extra_info(info))]
	schema := *(*Ref)(C.duckdb_init_get_bind_data(info))
	vecSize := int(C.duckdb_vector_size())
	extra_info := malloc(int(tfunc.InitScanner(schema, vecSize)))
	C.duckdb_init_set_init_data(info, extra_info, C.duckdb_delete_callback_t(C.free))
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

func getDuckdbTypeFromValue(v any) (C.duckdb_type, error) {
	switch v.(type) {
	case uuid.UUID:
		return C.DUCKDB_TYPE_UUID, nil
	case int64:
		return C.DUCKDB_TYPE_BIGINT, nil
	case int32:
		return C.DUCKDB_TYPE_INTEGER, nil
	case uint8:
		return C.DUCKDB_TYPE_UTINYINT, nil
	case uint32:
		return C.DUCKDB_TYPE_UINTEGER, nil
	case string:
		return C.DUCKDB_TYPE_VARCHAR, nil
	case bool:
		return C.DUCKDB_TYPE_BOOLEAN, nil
	case float64:
		return C.DUCKDB_TYPE_DOUBLE, nil
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
	chunkPool.Put(ch)
}

type DataChunk struct {
	Columns []Vector
}

type Vector struct {
	vector   C.duckdb_vector
	ptr      unsafe.Pointer
	pos      int
	uint64s  []uint64
	uint32s  []uint32
	uint16s  []uint16
	uint8s   []uint8
	float64s []float64
	bools    []bool
	uuids    [][16]uint8
	bitmask  *C.uint64_t
}

func (d *Vector) AppendUInt64(v uint64) {
	if d == nil {
		return
	}
	d.uint64s[d.pos] = v
	d.pos++
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
	copy(vec.uuids[vec.pos][:], v)
	vec.pos++
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
	if d.pos > n {
		log.Println("HMMM", d.pos, n)
		panic(1111)
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

func (d *Vector) AppendBytes(v []byte) {
	if d == nil {
		return
	}
	cstr := (*C.char)(unsafe.Pointer(&v[0]))
	C.duckdb_vector_assign_string_element_len(d.vector, C.ulonglong(d.pos), cstr, C.idx_t(len(v)))
	d.pos++
}

func (d *Vector) AppendNull() {
	if d == nil {
		return
	}
	C.duckdb_validity_set_row_invalid(d.bitmask, C.ulonglong(d.pos))
	d.pos++
}

func initVecSlice[T uint64 | uint32 | uint16 | uint8 | float64 | float32 | bool | [16]uint8](sl *[]T, ptr unsafe.Pointer, sz int) {
	*sl = (*[1 << 31]T)(ptr)[:sz]
}

func (d *Vector) init(sz int, v C.duckdb_vector) {
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

	C.duckdb_vector_ensure_validity_writable(v)
	d.bitmask = C.duckdb_vector_get_validity(v)
}

func malloc(v ...int) unsafe.Pointer {
	extra_info := C.malloc(C.ulong(len(v)) * C.ulong(1*unsafe.Sizeof(int(0))))
	for i, v := range v {
		(*[1024]int)(extra_info)[i] = v
	}
	return extra_info
}
