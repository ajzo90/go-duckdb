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
		Scan(chunk *DataChunk) int
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
		_, typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		value := C.duckdb_bind_get_parameter(info, C.ulonglong(i))
		arg, err := getValue(typ, value)
		if err != nil {
			udf_bind_error(info, err)
			return
		}
		args = append(args, arg)
	}

	schemaRef := tfunc.BindArguments(args...)
	schema := tfunc.GetSchema(schemaRef)
	for _, v := range schema.Columns {
		t, _, err := getDuckdbTypeFromValue(v.V)
		if err != nil {
			udf_bind_error(info, err)
			return
		}

		colName := C.CString(v.Name)
		C.duckdb_bind_add_result_column(info, colName, t)
		C.free(unsafe.Pointer(colName))
	}

	C.duckdb_bind_set_cardinality(info, C.ulonglong(schema.Cardinality), C.bool(schema.ExactCardinality))
	C.duckdb_bind_set_bind_data(info, malloc(int(schemaRef)), C.duckdb_delete_callback_t(nil))
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
	size := tblFunc.GetScanner(local).Scan(ch)
	releaseChunk(ch)
	C.duckdb_data_chunk_set_size(output, C.ulonglong(size))
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
			argtype, _, err := getDuckdbTypeFromValue(v)
			if err != nil {
				return err
			}
			C.duckdb_table_function_add_parameter(tableFunction, argtype)
		}

		state := C.duckdb_register_table_function(duckConn.duckdbCon, tableFunction)
		if state != 0 {
			return invalidTableFunctionError()
		}
		return nil
	})
}

func getDuckdbTypeFromValue(v any) (C.duckdb_logical_type, C.duckdb_type, error) {
	switch v.(type) {
	case int64:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), C.DUCKDB_TYPE_BIGINT, nil
	case string:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), C.DUCKDB_TYPE_VARCHAR, nil
	default:
		return C.duckdb_logical_type(nil), C.DUCKDB_TYPE_INVALID, unsupportedTypeError(reflect.TypeOf(v).String())
	}
}

func getValue(t C.duckdb_type, v C.duckdb_value) (any, error) {
	switch t {
	case C.DUCKDB_TYPE_BIGINT:
		return int64(C.duckdb_get_int64(v)), nil
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
	c.cscr = acquireCStr()
	for i := range c.Columns {
		c.Columns[i].init(vecSize, C.duckdb_data_chunk_get_vector(output, C.ulonglong(i)), c.cscr)
	}
	return c
}

func releaseChunk(ch *DataChunk) {
	releaseCStr(ch.cscr)
	chunkPool.Put(ch)
}

type DataChunk struct {
	Columns []Vector
	cscr    cstr
}

type Vector struct {
	vector   C.duckdb_vector
	ptr      unsafe.Pointer
	pos      int
	int64s   []int64
	uint64s  []uint64
	float64s []float64
	bools    []bool
	cscr     cstr
}

func (d *Vector) AppendInt64(v ...int64) {
	d.pos += copy(d.int64s[d.pos:], v)
}

func (d *Vector) AppendUInt64(v ...uint64) {
	d.pos += copy(d.uint64s[d.pos:], v)
}

func (d *Vector) AppendFloat64(v ...float64) {
	d.pos += copy(d.float64s[d.pos:], v)
}

func (d *Vector) AppendBool(v ...bool) {
	d.pos += copy(d.bools[d.pos:], v)
}

func (d *Vector) AppendBytes(v ...[]byte) {
	for _, v := range v {
		d.cscr.buf = append(d.cscr.buf[:0], v...)
		cstr := (*C.char)(unsafe.Pointer(&d.cscr.buf[0]))
		C.duckdb_vector_assign_string_element_len(d.vector, C.ulonglong(d.pos), cstr, C.idx_t(len(v)))
		d.pos++
	}
}

func (d *Vector) SetNull(idx int) {
	panic("not implemented")
}

func initVecSlice[T int64 | uint64 | float64 | bool](sl *[]T, ptr unsafe.Pointer, sz int) {
	*sl = (*[1 << 31]T)(ptr)[:sz]
}

func (d *Vector) init(sz int, v C.duckdb_vector, scr cstr) {
	d.pos = 0
	d.vector = v
	d.ptr = C.duckdb_vector_get_data(v)
	d.cscr = scr
	initVecSlice(&d.uint64s, d.ptr, sz)
	initVecSlice(&d.int64s, d.ptr, sz)
	initVecSlice(&d.float64s, d.ptr, sz)
	initVecSlice(&d.bools, d.ptr, sz)
}

type cstr struct {
	buf []byte
}

var cbufpool struct {
	mtx    sync.Mutex
	allocs []cstr
}

func acquireCStr() cstr {
	var sl []byte
	b := &cbufpool
	b.mtx.Lock()
	if len(b.allocs) > 0 {
		sl = b.allocs[len(b.allocs)-1].buf[:0]
		b.allocs = b.allocs[:len(b.allocs)-1]
	}
	b.mtx.Unlock()

	if cap(sl) == 0 {
		sl = (*[1 << 31]byte)(C.malloc(C.ulong(4096 * unsafe.Sizeof(byte(0)))))[:0:4096]
	}

	return cstr{buf: sl}
}

func releaseCStr(b cstr) {
	cbufpool.mtx.Lock()
	defer cbufpool.mtx.Unlock()
	cbufpool.allocs = append(cbufpool.allocs, b)
}

func malloc(v ...int) unsafe.Pointer {
	extra_info := C.malloc(C.ulong(len(v)) * C.ulong(1*unsafe.Sizeof(int(0))))
	for i, v := range v {
		(*[1024]int)(extra_info)[i] = v
	}
	return extra_info
}
