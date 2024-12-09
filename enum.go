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
	for _, v := range e.Names() {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return b
}

func (e *Enum) tryDeserialize(b []byte) {
	for len(b) > 0 {
		sz, n := binary.Uvarint(b)
		if n <= 0 {
			panic("invalid uvarint")
		}
		b = b[n:]
		e.Register(b[:sz])
		b = b[sz:]
	}
}

func NewEnum(v []byte) *Enum {
	e := &Enum{
		values: make([]string, 0, 1024),
		m:      make(map[uint64]uint32),
	}
	e.tryDeserialize(v)
	return e
}

func (e *Enum) Names() []string {
	e.mtx.RLock()
	defer e.mtx.RUnlock()
	return e.values
}

func (e *Enum) add(hash uint64, s []byte) uint32 {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if id, ok := e.m[hash]; ok {
		return id
	}

	id := uint32(len(e.values))
	v := string(s)
	e.values = append(e.values, v)
	e.m[hash] = id
	return id
}

func (e *Enum) Register(b []byte) uint32 {
	x := xxhash.Sum64(b)
	return e.RegisterHash(x, b)
}

func (e *Enum) RegisterHash(hash uint64, b []byte) uint32 {
	e.mtx.RLock()
	id, ok := e.m[hash]
	e.mtx.RUnlock()
	if ok {
		return id
	}
	return e.add(hash, b)
}
