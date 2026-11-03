package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"sync"
)

// interruptOnDone interrupts con when ctx ends. The returned stop must be
// called before con runs another query, so a late interrupt cannot cancel it;
// it waits for the watcher to exit.
func interruptOnDone(ctx context.Context, con C.duckdb_connection) (stop func()) {
	done, exited := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			C.duckdb_interrupt(con)
		case <-done:
		}
	}()
	return sync.OnceFunc(func() {
		close(done)
		<-exited
	})
}
