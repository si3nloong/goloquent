package goloquent

import (
	"bytes"
	"database/sql"
	"strings"
	"time"
)

type stmt struct {
	statement *bytes.Buffer
	arguments []interface{}
}

func (s stmt) string() string {
	return s.statement.String()
}

func (s stmt) isZero() bool {
	return !(s.statement.Len() > 0)
}

type replacer interface {
	Bind(uint) string
	Value(interface{}) string
}

// Stmt :
type Stmt struct {
	stmt
	crud      string
	replacer  replacer
	startTime time.Time
	endTime   time.Time
	Result    sql.Result
}

func (s *Stmt) startTrace() {
	s.startTime = time.Now().UTC()
}

func (s *Stmt) stopTrace() {
	s.endTime = time.Now().UTC()
}

// TimeElapse :
func (s Stmt) TimeElapse() time.Duration {
	return s.endTime.Sub(s.startTime)
}

// Raw :
func (s *Stmt) Raw() string {
	buf := new(bytes.Buffer)
	if len(s.arguments) <= 0 {
		return s.string()
	}
	arr := strings.Split(s.string(), variable)
	for i := 0; i < len(arr); i++ {
		str := arr[i] + s.replacer.Bind(uint(i+1))
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
	arr := strings.Split(s.string(), variable)
	for i, aa := range s.arguments {
		str := arr[i] + s.replacer.Value(aa)
		buf.WriteString(str)
	}
	buf.WriteString(arr[len(arr)-1])
	return buf.String()
}

// Arguments :
func (s Stmt) Arguments() []interface{} {
	return s.arguments
}
