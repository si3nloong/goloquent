package goloquent

import (
	"io"
	"sync"
)

var (
	sqlStmtPool = sync.Pool{
		New: func() interface{} {
			return new(sqlStmt)
		},
	}
)

func acquireStmt() *sqlStmt {
	return sqlStmtPool.Get().(*sqlStmt)
}

func releaseStmt(stmt *sqlStmt) {
	sqlStmtPool.Put(stmt)
}

type sqlStmt struct {
	w    io.Writer
	args []interface{}
}
