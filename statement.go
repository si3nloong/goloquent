package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

type stmt struct {
	statement *bytes.Buffer
	arguments []interface{}
}

func (s *stmt) string() string {
	return s.statement.String()
}

type binder func(uint) string

// Stmt :
type Stmt struct {
	stmt
	Result   sql.Result
	replacer binder
}

func (s *Stmt) startTrace() {

}

// Raw :
func (s *Stmt) Raw() string {
	buf := new(bytes.Buffer)
	arr := strings.Split(s.string(), "??")
	for i := 0; i < len(arr); i++ {
		str := arr[i] + s.replacer(uint(i+1))
		if i >= len(arr)-1 {
			str = arr[i]
		}
		buf.WriteString(str)
	}
	return buf.String()
}

// String :
func (s *Stmt) String() string {
	buf := new(bytes.Buffer)
	arr := strings.Split(s.string(), "??")
	for i, aa := range s.arguments {
		str := arr[i] + toString(aa)
		buf.WriteString(str)
	}
	buf.WriteString(arr[len(arr)-1])
	return buf.String()
}

// Arguments :
func (s *Stmt) Arguments() []interface{} {
	return s.arguments
}

func toString(it interface{}) string {
	var str string
	switch vi := it.(type) {
	case nil:
		str = "NULL"
	case string, []byte:
		str = fmt.Sprintf("%q", vi)
	default:
		str = fmt.Sprintf("%v", vi)
	}
	return str
}
