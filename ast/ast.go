package ast

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"github.com/valyala/fastjson"
	"golang.org/x/exp/slices"
	"io"
	"strings"
	"sync"
)

func Query(db *duckdb.Conn, q string, args []driver.NamedValue, tojson bool, fn func(row []byte) error) error {
	if tojson {
		q = fmt.Sprintf(`SELECT json_array(*columns(*)) FROM (FROM (%s) SELECT to_json(columns(*)))`, q)
	}
	rows, err := db.ExtendedQueryContext(context.Background(), q, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	var ch duckdb.Chunk

	for {
		if err := rows.NextChunk(&ch); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		vec, err := ch.Varchar(0)
		if err != nil {
			return err
		}
		for _, v := range vec {
			if err := fn(v.Bytes()); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil && err != io.EOF {
		return err
	} else if err := rows.Close(); err != nil {
		return err
	} else {
		return nil
	}
}

func QueryRow(db *duckdb.Conn, q string, args []driver.NamedValue, tojson bool) ([]byte, error) {
	var res []byte
	var err = Query(db, fmt.Sprintf("FROM (%s) LIMIT 1", q), args, tojson, func(row []byte) error {
		if len(res) > 0 {
			panic("expected 1 row")
		}
		res = append(res, row...)
		return nil
	})
	return res, err
}

func QueryAST(db *duckdb.Conn, q string) ([]byte, error) {
	return QueryRow(db, "select json_serialize_sql(?::VARCHAR, skip_empty := false, skip_null := true, format := true)::VARCHAR", []driver.NamedValue{{Ordinal: 1, Value: q}}, false)
}
func RenderQuery(db *duckdb.Conn, q string) ([]byte, error) {
	return QueryRow(db, "select json_deserialize_sql(?)", []driver.NamedValue{{Ordinal: 1, Value: q}}, false)
}

func cloneJSON(ast *fastjson.Value) *fastjson.Value {
	ser := ast.MarshalTo(make([]byte, 0, 512))
	ast, _ = fastjson.ParseBytes(ser)
	return ast
}

func ParseExpression(v *fastjson.Value) (*fastjson.Value, error) {

	var arr = v.GetArray("statements")
	if len(arr) != 1 {
		return nil, fmt.Errorf("invalid expression '%s'", v.MarshalTo(nil))
	}

	node := arr[0].Get("node")
	if string(node.GetStringBytes("type")) != "SELECT_NODE" {
		return nil, fmt.Errorf("invalud expression 222")
	}
	sel := node.GetArray("select_list")
	if len(sel) != 1 {
		return nil, fmt.Errorf("asdfsdfds%d", len(sel))
	}
	return sel[0], nil
}

const (
	TYPE_COLUMN_REF = "COLUMN_REF"
	TYPE_FUNCTION   = "FUNCTION"
	VALUE_PARAMETER = "VALUE_PARAMETER"
)

func columnRef(value *fastjson.Value) []string {
	if v := value.GetArray("column_names"); len(v) > 0 {
		var arr []string
		for _, v := range v {
			arr = append(arr, string(v.GetStringBytes()))
		}
		return arr
	} else {
		return nil
	}
}

func Ast(db *duckdb.Conn, q string) (*fastjson.Value, error) {
	jsonRow, err := QueryAST(db, q)
	if err != nil {
		return nil, err
	}
	//fmt.Println(string(jsonRow))
	v, err := fastjson.ParseBytes(jsonRow)
	if err != nil {
		return nil, err
	} else if isErr := v.GetBool("error"); isErr {
		return nil, fmt.Errorf("%s", v.GetStringBytes("error_message"))
	}

	if len(v.GetArray("statements")) == 1 {
		return v, nil
	}
	return nil, fmt.Errorf("not found")
}

func WalkJSON(v *fastjson.Value, f func(v *fastjson.Value) *fastjson.Value) *fastjson.Value {
	v = f(v)
	switch v.Type() {
	case fastjson.TypeArray:
		for i, arrV := range v.GetArray() {
			v.SetArrayItem(i, WalkJSON(arrV, f))
		}
	case fastjson.TypeObject:
		var o = v.GetObject()
		o.Visit(func(key []byte, v *fastjson.Value) {
			o.Set(string(key), WalkJSON(v, f))
		})
	}
	return v
}

type expression struct {
	Definition string
	Value      *fastjson.Value
	err        error
	mtx        sync.Mutex
	params     map[string]dataType
}

var NewExpression = func(s string) *expression {
	return &expression{Definition: s}
}

func astResolve(ast *fastjson.Value, getExpression func(string) *expression, getFunction func(v *fastjson.Value) (*fastjson.Value, error), conn *duckdb.Conn, isAggFunc func(string) bool) (*fastjson.Value, error) {
	var resolveColumn = func(v *fastjson.Value) (*fastjson.Value, error) {

		var fullName = strings.Join(columnRef(v), ".")
		if e := getExpression(fullName); e == nil {
			return nil, fmt.Errorf("expression [%s] not found", fullName)
		} else if e.Definition != fullName {
			e2, err := e.resolveOrParse(getExpression, getFunction, conn, isAggFunc)
			if err != nil {
				return nil, err
			}
			return ParseExpression(e2)
		} else {
			return v, nil
		}
	}

	var firstErr error
	var walkFn = func(v *fastjson.Value) *fastjson.Value {
		if v == nil || firstErr != nil {
			return v
		}

		typ := v.GetStringBytes("type")
		class := v.GetStringBytes("class")
		if !bytes.Equal(typ, class) {
			return v
		}

		var newVal *fastjson.Value
		switch string(typ) {
		default:
			return v
		case TYPE_COLUMN_REF:
			newVal, firstErr = resolveColumn(v)
		case TYPE_FUNCTION:
			newVal, firstErr = getFunction(v)
		}
		return newVal
	}
	return WalkJSON(ast, walkFn), firstErr
}

func (e *expression) resolveOrParse(expressions func(string) *expression, macros func(v *fastjson.Value) (*fastjson.Value, error), conn *duckdb.Conn, isAggFunc func(string) bool) (*fastjson.Value, error) {
	if !e.mtx.TryLock() {
		return nil, fmt.Errorf("circular reference")
	}
	defer e.mtx.Unlock()

	if e.Value != nil || e.err != nil {
		return e.Value, e.err
	}

	ast, err := Ast(conn, fmt.Sprintf("select (%s)", e.Definition))
	if err != nil {
		return nil, err
	}

	WalkJSON(ast, func(v *fastjson.Value) *fastjson.Value {
		switch string(v.GetStringBytes("type")) {
		case VALUE_PARAMETER:
			identifier := string(v.GetStringBytes("identifier"))
			var a fastjson.Arena
			switch identifier {
			default:
				//todo: pass params to expression definition
				return e.params[identifier].render(&a)
			case "foo":
				return newStrType("ba'r").render(&a)
			case "bar":
				return newIntType(123).render(&a)
			}
		}
		return v
	})

	e.Value, e.err = astResolve(ast, expressions, macros, conn, isAggFunc)
	return e.Value, e.err
}

type dbMeta struct {
	aggregateFunctions []string
	mtx                sync.Mutex
}

func (m *dbMeta) GetAggregateFunctions(conn *duckdb.Conn) []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.aggregateFunctions) == 0 {
		err := Query(conn, `select distinct function_name from duckdb_functions() where function_type='aggregate';`, nil, false, func(row []byte) error {
			m.aggregateFunctions = append(m.aggregateFunctions, string(row))
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	return m.aggregateFunctions
}

var Rewrite = func(conn *duckdb.Conn, q string, getExpression func(string) *expression, resolveFunction func(value *fastjson.Value) (*fastjson.Value, error), isAggFunc func(string) bool, a *fastjson.Arena) ([]byte, error) {
	ast, err := Ast(conn, q)
	if err != nil {
		return nil, err
	}

	rr, err := astResolve(ast, getExpression, resolveFunction, conn, isAggFunc)
	if err != nil {
		return nil, err
	}

	return RenderQuery(conn, string(rr.MarshalTo(nil)))

}

type rewriter struct {
	isAggFunc         func(string) bool
	mtx               sync.Mutex
	conn              *duckdb.Conn
	a                 fastjson.Arena
	staticExpressions expressionResolver
}

type expressionResolver struct {
	expressions    map[string]*expression
	srcExpressions map[string]string
}

func (r *rewriter) getExpression(s string) *expression {
	return r.staticExpressions.getExpression(s)
}

func (r *expressionResolver) getExpression(s string) *expression {
	v, ok := r.expressions[s]
	if ok {
		return v
	}
	e, ok := r.srcExpressions[s]
	if ok {
		r.expressions[s] = NewExpression(e)
	} else {
		// add name as a field/identity.
		r.expressions[s] = NewExpression(s)
	}
	return r.expressions[s]
}

func newExpressionResolver(exps map[string]string) expressionResolver {
	return expressionResolver{srcExpressions: exps, expressions: make(map[string]*expression)}
}

func NewRewriter(expressions map[string]string) *rewriter {
	return &rewriter{staticExpressions: newExpressionResolver(expressions)}
}

func (r *rewriter) resolveFunction(v *fastjson.Value) (*fastjson.Value, error) {
	var functionName = string(v.GetStringBytes("function_name"))
	var a fastjson.Arena
	if functionName == "uniq" {
		v = cloneJSON(v)
		v.Set("function_name", a.NewString("count"))
		v.Set("distinct", a.NewTrue())
	}
	var children = v.GetArray("children")
	if functionName == "aggregate_conjunction" && len(children) == 2 {
		x, err := astResolve(children[0], r.getExpression, r.resolveFunction, r.conn, r.isAggFunc)
		if err != nil {
			return v, err
		}
		pred, err := astResolve(children[1], r.getExpression, r.resolveFunction, r.conn, r.isAggFunc)
		if err != nil {
			return v, err
		}
		return aggregateConjunction(x, pred, r.isAggFunc)
	}
	return v, nil
}

func aggregateConjunction(ast, paramF *fastjson.Value, isAggFunc func(s string) bool) (*fastjson.Value, error) {

	var walkFn = func(v *fastjson.Value) *fastjson.Value {
		if !isAggFunc(string(v.GetStringBytes("function_name"))) {
			return v
		}
		if filter := v.Get("filter"); filter == nil {
			v.Set("filter", paramF)
		} else {
			var a fastjson.Arena
			newFilter := a.NewObject()
			newFilter.Set("class", a.NewString("CONJUNCTION"))
			newFilter.Set("type", a.NewString("CONJUNCTION_AND"))
			var children = a.NewArray()
			children.SetArrayItem(0, filter)
			children.SetArrayItem(1, paramF)
			newFilter.Set("children", children)
			v.Set("filter", newFilter)
		}
		return v
	}

	return WalkJSON(cloneJSON(ast), walkFn), nil
}

func (r *rewriter) Rewrite(conn *duckdb.Conn, q string) ([]byte, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.conn = conn
	r.a.Reset()

	if r.isAggFunc == nil {
		var aggregateFunctions = duckMeta.GetAggregateFunctions(conn)
		r.isAggFunc = func(s string) bool {
			return slices.Contains(aggregateFunctions, s)
		}
	}

	return Rewrite(conn, q, r.getExpression, r.resolveFunction, r.isAggFunc, &r.a)
}

var duckMeta dbMeta

type dataType struct {
	valueFn func(a *fastjson.Arena) *fastjson.Value
	typ     string
}

func newType(typ string, fn func(a *fastjson.Arena) *fastjson.Value) dataType {
	return dataType{
		valueFn: fn,
		typ:     typ,
	}
}

func newIntType(v int) dataType {
	return newType("INTEGER", func(a *fastjson.Arena) *fastjson.Value {
		return a.NewNumberInt(v)
	})
}

func newStrType(s string) dataType {
	return newType("VARCHAR", func(a *fastjson.Arena) *fastjson.Value {
		return a.NewString(s)
	})
}

func (v dataType) render(a *fastjson.Arena) *fastjson.Value {
	var isNull = v.valueFn == nil
	if v.valueFn == nil {
		v = newType("INTEGER", func(a *fastjson.Arena) *fastjson.Value {
			return a.NewNull()
		})
	}
	o := a.NewObject()
	o.Set("class", a.NewString("CONSTANT"))
	o.Set("type", a.NewString("VALUE_CONSTANT"))
	o.Set("alias", a.NewString(""))

	typ := a.NewObject()
	typ.Set("id", a.NewString(v.typ))

	val := a.NewObject()
	if isNull {
		val.Set("is_null", a.NewTrue())
	} else {
		val.Set("is_null", a.NewFalse())
	}
	val.Set("value", v.valueFn(a))
	val.Set("type", typ)

	o.Set("value", val)
	return o
}
