package duckdb

import (
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
