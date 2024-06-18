package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"sync"
	"unsafe"
)

var cMem = mempool{
	m: make(map[ref]any),
}

type ref uint64

type mempool struct {
	m   map[ref]any
	ptr ref
	mtx sync.Mutex
}

func (h *mempool) free(ref *ref) {
	h.mtx.Lock()
	if _, exist := h.m[*ref]; !exist {
		panic("invalid free")
	}
	delete(h.m, *ref)
	h.mtx.Unlock()
	C.duckdb_free(unsafe.Pointer(ref))
}

func (h *mempool) lookup(ref *ref) any {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	return h.m[*ref]
}

func (h *mempool) store(v any) unsafe.Pointer {

	h.mtx.Lock()
	ptr := h.ptr
	h.m[ptr] = v
	h.ptr++
	h.mtx.Unlock()

	x := C.duckdb_malloc(C.size_t(unsafe.Sizeof(ref(0))))
	*(*ref)(x) = ptr

	return x
}
