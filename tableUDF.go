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
	"reflect"
	"time"
	"unsafe"
)

type (
	ColumnDef struct {
		Name string
		Type string
	}
	Binding interface {
		Table() *Table
		InitScanner(vecSize int, projection []int) Scanner
	}
	Table struct {
		Name             string // useful?
		Columns          []ColumnDef
		MaxThreads       int
		Cardinality      int
		ExactCardinality bool
	}
	Scanner interface {
		Scan(chunk *UDFDataChunk) (int, error)
		Close()
	}
	TableFunction interface {
		Arguments() []any
		NamedArguments() map[string]any
		Bind(named map[string]any, args []any) (Binding, error)
	}
)

type bindValue struct {
	binding    Binding
	projection []int
}

func RegisterTableUDF(c *sql.Conn, name string, function TableFunction) error {
	return c.Raw(func(driverConn any) error {
		conn, ok := driverConn.(driver.Conn)
		if !ok {
			return driver.ErrBadConn
		}
		return RegisterTableUDFConn(conn, name, function)
	})
}

func RegisterTableUDFConn(c driver.Conn, _name string, function TableFunction) error {
	duckConn, err := getConn(c)
	if err != nil {
		return err
	}
	name := C.CString(_name)
	defer C.free(unsafe.Pointer(name))

	tableFunction := C.duckdb_create_table_function()
	C.duckdb_table_function_set_name(tableFunction, name)
	C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind))
	C.duckdb_table_function_set_init(tableFunction, C.init(C.udf_init))
	C.duckdb_table_function_set_local_init(tableFunction, C.init(C.udf_local_init))
	C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_callback))
	C.duckdb_table_function_supports_projection_pushdown(tableFunction, C.bool(true))
	C.duckdb_table_function_set_extra_info(tableFunction, cMem.store(function), C.duckdb_delete_callback_t(C.udf_destroy_data))

	for _, v := range function.Arguments() {
		lt, err := createLogicalFromSQLType(SqlTypeFromValue(v))
		if err != nil {
			return err
		}
		C.duckdb_table_function_add_parameter(tableFunction, lt)
		C.duckdb_destroy_logical_type(&lt)
	}

	for name, v := range function.NamedArguments() {
		lt, err := createLogicalFromSQLType(SqlTypeFromValue(v))
		if err != nil {
			return err
		}
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

//export udf_bind
func udf_bind(info C.duckdb_bind_info) {
	if err := _udf_bind(info); err != nil {
		errstr := C.CString(err.Error())
		C.duckdb_bind_set_error(info, errstr)
		C.free(unsafe.Pointer(errstr))
	}
}

func _udf_bind(info C.duckdb_bind_info) error {
	ref := (*ref)(C.duckdb_bind_get_extra_info(info))
	tblFunc := cMem.lookup(ref).(TableFunction)

	var args []any
	for i, v := range tblFunc.Arguments() {
		typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			return err
		}
		value := C.duckdb_bind_get_parameter(info, C.uint64_t(i))
		arg, err := getBindValue(typ, value)
		C.duckdb_destroy_value(&value)
		if err != nil {
			return err
		}
		args = append(args, arg)
	}

	namedArgs := make(map[string]any)
	for name, v := range tblFunc.NamedArguments() {
		typ, err := getDuckdbTypeFromValue(v)
		if err != nil {
			return err
		}
		argName := C.CString(name)
		value := C.duckdb_bind_get_named_parameter(info, argName)
		C.free(unsafe.Pointer(argName))
		arg, err := getBindValue(typ, value)
		C.duckdb_destroy_value(&value)
		if err != nil {
			return err
		}
		namedArgs[name] = arg
	}

	bind, err := tblFunc.Bind(namedArgs, args)
	if err != nil {
		return err
	}
	table := bind.Table()

	var addCol = func(name string, typ C.duckdb_logical_type) {
		colName := C.CString(name)
		C.duckdb_bind_add_result_column(info, colName, typ)
		C.free(unsafe.Pointer(colName))
	}

	for _, v := range table.Columns {
		logical, err := createLogicalFromSQLType(v.Type)
		if err != nil {
			return err
		}
		addCol(v.Name, logical)
		C.duckdb_destroy_logical_type(&logical)
	}

	C.duckdb_bind_set_cardinality(info, C.uint64_t(table.Cardinality), C.bool(table.ExactCardinality))
	C.duckdb_bind_set_bind_data(info, cMem.store(&bindValue{binding: bind}), C.duckdb_delete_callback_t(C.udf_destroy_data))
	return nil
}

//export udf_destroy_data
func udf_destroy_data(data unsafe.Pointer) {
	ref := (*ref)(data)
	cMem.free(ref)
}

func createEnum(names []string) C.duckdb_logical_type {
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
	p := (**C.char)(malloc(ptrs...))
	typ := C.duckdb_create_enum_type(p, C.idx_t(len(ptrs)))
	C.free(colName)
	C.duckdb_free(unsafe.Pointer(p))
	return typ
}

func malloc(strs ...unsafe.Pointer) unsafe.Pointer {
	x := C.duckdb_malloc(C.size_t(len(strs)) * C.size_t(8))
	for i, v := range strs {
		(*[1 << 31]unsafe.Pointer)(x)[i] = v
	}
	return x
}

//export udf_init
func udf_init(info C.duckdb_init_info) {
	count := int(C.duckdb_init_get_column_count(info))
	ref := getBind(info)
	bind := cMem.lookup(ref).(*bindValue)
	table := bind.binding.Table()

	bind.projection = make([]int, count)
	for i := 0; i < count; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, C.uint64_t(i)))
		bind.projection[i] = srcPos
	}
	C.duckdb_init_set_max_threads(info, C.uint64_t(table.MaxThreads))
}

//export udf_local_init_cleanup
func udf_local_init_cleanup(info C.duckdb_init_info) {
	ref := (*ref)(unsafe.Pointer(info))
	cMem.lookup(ref).(Scanner).Close()
	cMem.free(ref)
}

func getBind(info C.duckdb_init_info) *ref {
	return (*ref)(C.duckdb_init_get_bind_data(info))
}

func getScanner(info C.duckdb_function_info) *ref {
	return (*ref)(C.duckdb_function_get_local_init_data(info))
}

//export udf_local_init
func udf_local_init(info C.duckdb_init_info) {
	ref := getBind(info)
	bind := cMem.lookup(ref).(*bindValue)
	vecSize := int(C.duckdb_vector_size())
	scanner := bind.binding.InitScanner(vecSize, bind.projection)
	C.duckdb_init_set_init_data(info, cMem.store(scanner), C.duckdb_delete_callback_t(C.udf_local_init_cleanup))
}

//export udf_callback
func udf_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	vecSize := int(C.duckdb_vector_size())
	scanner := cMem.lookup(getScanner(info)).(Scanner)

	ch := acquireChunk(vecSize, output)
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

func getDuckdbTypeFromValueX(v any) C.duckdb_type {
	switch v.(type) {
	case uuid.UUID:
		return C.DUCKDB_TYPE_UUID
	case uint64:
		return C.DUCKDB_TYPE_UBIGINT
	case int32:
		return C.DUCKDB_TYPE_INTEGER
	case int16:
		return C.DUCKDB_TYPE_SMALLINT
	case int8:
		return C.DUCKDB_TYPE_TINYINT
	case int64:
		return C.DUCKDB_TYPE_BIGINT
	case uint32:
		return C.DUCKDB_TYPE_UINTEGER
	case uint16:
		return C.DUCKDB_TYPE_USMALLINT
	case uint8:
		return C.DUCKDB_TYPE_UTINYINT
	case string:
		return C.DUCKDB_TYPE_VARCHAR
	case bool:
		return C.DUCKDB_TYPE_BOOLEAN
	case float64:
		return C.DUCKDB_TYPE_DOUBLE
	case float32:
		return C.DUCKDB_TYPE_FLOAT
	case time.Time:
		return C.DUCKDB_TYPE_TIMESTAMP
	default:
		return C.DUCKDB_TYPE_INVALID
	}
}

func SqlTypeFromValue(v any) string {
	if _, ok := v.([]string); ok {
		return "VARCHAR[]"
	} else if _, ok := v.([]uint32); ok {
		return "UINTEGER[]"
	} else if _, ok := v.([]float64); ok {
		return "DOUBLE[]"
	} else if enum, ok := v.(*Enum); ok {
		return StringifyEnum(enum.values)
	} else {
		return duckdbTypeMap[getDuckdbTypeFromValueX(v)]
	}
}

func getDuckdbTypeFromValue(v any) (C.duckdb_type, error) {
	x := getDuckdbTypeFromValueX(v)
	if x == C.DUCKDB_TYPE_INVALID {
		x := reflect.TypeOf(v)
		if x == nil {
			panic(1)
		}
		return C.DUCKDB_TYPE_INVALID, unsupportedTypeError(x.String())
	}
	return x, nil
}

func getBindValue(typ C.duckdb_type, v C.duckdb_value) (any, error) {

	switch typ {
	case C.DUCKDB_TYPE_BOOLEAN:
		if v == nil {
			return false, nil
		} else if C.duckdb_get_int64(v) != 0 {
			return true, nil
		} else {
			return false, nil
		}
	case C.DUCKDB_TYPE_BIGINT:
		if v == nil {
			return int64(0), nil
		}
		return int64(C.duckdb_get_int64(v)), nil
	case C.DUCKDB_TYPE_DOUBLE:
		panic("not implemented")
	case C.DUCKDB_TYPE_VARCHAR:
		if v == nil {
			return "", nil
		}
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
