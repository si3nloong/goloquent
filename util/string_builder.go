package util

import (
	"strings"
	"sync"
	"unsafe"
)

var (
	strBldrPool = &sync.Pool{
		New: func() interface{} {
			return new(strings.Builder)
		},
	}
)

// AcquireString :
func AcquireString() *strings.Builder {
	return strBldrPool.Get().(*strings.Builder)
}

// ReleaseString :
func ReleaseString(x *strings.Builder) {
	if x != nil {
		x.Reset()
		strBldrPool.Put(x)
	}
}

// UnsafeString :
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
