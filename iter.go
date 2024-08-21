package duckdb

import (
	"database/sql/driver"
	"iter"
)

type iterFn[T any] struct {
	t    *Table
	it   iter.Seq[T]
	scan func(chunk *UDFDataChunk, next func() (T, bool)) int
}

func (i *iterFn[T]) Arguments() []any {
	return nil
}

func (i *iterFn[T]) NamedArguments() map[string]any {
	return nil
}
func (i *iterFn[T]) Bind(named map[string]any, args []any) (Binding, error) {
	return &iterFnBind[T]{iter: i.it, p: i}, nil
}

type iterFnBind[T any] struct {
	p    *iterFn[T]
	iter iter.Seq[T]
}

func (i *iterFnBind[T]) Table() *Table {
	return i.p.t
}

type iterFnScanner[T any] struct {
	next func() (T, bool)
	stop func()
	fn   *iterFn[T]
	proj []int
}

func (i *iterFnScanner[T]) Scan(chunk *UDFDataChunk) (int, error) {
	return i.fn.scan(chunk, i.next), nil
}

func (i *iterFnScanner[T]) Close() {
	i.stop()
}

func (i *iterFnBind[T]) InitScanner(vecSize int, projection []int) Scanner {
	next, stop := iter.Pull(i.iter)
	return &iterFnScanner[T]{next: next, stop: stop, fn: i.p, proj: projection}
}

func IterTableFunc[T any](it iter.Seq[T], mapper func(chunk *UDFDataChunk, v T), defs ...ColumnDef) TableFunction {
	t := &Table{Columns: defs, MaxThreads: 1}
	return &iterFn[T]{t: t, it: it, scan: func(chunk *UDFDataChunk, next func() (T, bool)) int {
		for i := 0; i < chunk.Capacity; i++ {
			vv, ok := next()
			if !ok {
				return i
			}
			mapper(chunk, vv)
		}
		return chunk.Capacity
	}}
}

type Tuple[T1, T2, T3, T4, T5, T6 validTypes] struct {
	V1 T1
	V2 T2
	V3 T3
	V4 T4
	V5 T5
	V6 T6
}

func RegisterUDFFromIterator[T1, T2, T3, T4, T5, T6 validTypes](conn driver.Conn, name string, iter iter.Seq[Tuple[T1, T2, T3, T4, T5, T6]]) error {
	return RegisterTableUDFConnPushdown(conn, name, _UDFFromIterator(iter), false)
}

func _UDFFromIterator[T1, T2, T3, T4, T5, T6 validTypes](iter iter.Seq[Tuple[T1, T2, T3, T4, T5, T6]]) TableFunction {

	var names = []string{"v1", "v2", "v3", "v4", "v5", "v6"}

	return IterTableFunc(iter, func(ch *UDFDataChunk, v Tuple[T1, T2, T3, T4, T5, T6]) {
		Append(&ch.Columns[0], v.V1)
		Append(&ch.Columns[1], v.V2)
		Append(&ch.Columns[2], v.V3)
		Append(&ch.Columns[3], v.V4)
		Append(&ch.Columns[4], v.V5)
		Append(&ch.Columns[5], v.V6)
	},
		ColDef(names[0], SqlTypeFromValue(*new(T1))),
		ColDef(names[1], SqlTypeFromValue(*new(T2))),
		ColDef(names[2], SqlTypeFromValue(*new(T3))),
		ColDef(names[3], SqlTypeFromValue(*new(T4))),
		ColDef(names[4], SqlTypeFromValue(*new(T5))),
		ColDef(names[5], SqlTypeFromValue(*new(T6))),
	)
}
