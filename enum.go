package duckdb

import (
	"encoding/binary"
	"github.com/cespare/xxhash"
	"sync"
)

type Enum struct {
	values []string
	m      map[uint64]uint32
	mtx    sync.RWMutex
}

func (e *Enum) Serialize(b []byte) []byte {
	for _, v := range e.values {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return b
}

func DeserializeEnum(b []byte) *Enum {
	e := NewEnum()
	for len(b) > 0 {
		sz, n := binary.Uvarint(b)
		if n <= 0 {
			panic("invalid uvarint")
		}
		b = b[n:]
		e.Register(b[:sz])
		b = b[sz:]
	}
	return e
}

func NewEnum() *Enum {
	return &Enum{
		values: make([]string, 0, 1024),
		m:      make(map[uint64]uint32),
	}
}

func (e *Enum) Names() []string {
	e.mtx.RLock()
	defer e.mtx.RUnlock()
	return e.values
}

func (e *Enum) add(x uint64, s []byte) uint32 {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if id, ok := e.m[x]; ok {
		return id
	}

	id := uint32(len(e.values))
	v := string(s)
	e.values = append(e.values, v)
	e.m[x] = id
	return id
}

func (e *Enum) Register(b []byte) uint32 {
	x := xxhash.Sum64(b)
	e.mtx.RLock()
	id, ok := e.m[x]
	e.mtx.RUnlock()
	if ok {
		return id
	}
	return e.add(x, b)
}
