package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

// Stmt :
type Stmt struct {
	table     string
	statement *bytes.Buffer
	arguments []interface{}
	Result    sql.Result
}

// Raw :
func (s *Stmt) Raw() string {
	return s.statement.String()
}

// String :
func (s *Stmt) String() string {
	str := strings.Replace(s.Raw(), "?", "?|", -1)
	arr := strings.Split(str, "|")
	for i, aa := range s.arguments {
		if i >= len(arr) {
			break
		}
		arr[i] = strings.Replace(arr[i], "?", toString(aa), 1)
	}
	return strings.Join(arr, "")
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
