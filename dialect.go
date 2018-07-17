package goloquent

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// Writer :
type Writer interface {
	Len() int
	Reset()
	io.Writer
	WriteRune(rune) (int, error)
	WriteString(s string) (n int, err error)
	fmt.Stringer
}

// Dialect :
type Dialect interface {
	Open(c Config) (*sql.DB, error)
	SetDB(db Client)
	GetTable(ns string) string
	Version() (ver string)
	CurrentDB() (n string)
	Quote(n string) string
	Bind(i uint) string
	FilterJSON(f Filter) (s string, args []interface{}, err error)
	JSONMarshal(i interface{}) (b json.RawMessage)
	Value(v interface{}) string
	GetSchema(c Column) []Schema
	DataType(s Schema) string
	HasTable(tb string) bool
	HasIndex(tb, idx string) bool
	GetColumns(tb string) (cols []string)
	GetIndexes(tb string) (idxs []string)
	CreateTable(tb string, cols []Column) error
	AlterTable(tb string, cols []Column) error
	OnConflictUpdate(tb string, cols []string) string
	UpdateWithLimit() bool
}

var (
	dialects = make(map[string]Dialect)
)

// RegisterDialect :
func RegisterDialect(driver string, d Dialect) {
	dialects[driver] = d
}

// GetDialect :
func GetDialect(driver string) (d Dialect, isValid bool) {
	d, isValid = dialects[driver]
	if isValid {
		// Clone a new dialect
		d = reflect.New(reflect.TypeOf(d).Elem()).Interface().(Dialect)
	}
	return
}
