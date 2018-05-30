package goloquent

import (
	"database/sql"
	"reflect"
)

// Dialect :
type Dialect interface {
	Open(c Config) (*sql.DB, error)
	SetDB(db Client)
	GetTable(ns string) string
	CreateIndex(ns string, cols []string) string
	Version() (ver string)
	CurrentDB() (n string)
	Quote(n string) string
	Bind(i int) string
	GetSchema(c Column) []Schema
	DataType(s Schema) string
	HasTable(tb string) bool
	GetColumns(tb string) (cols []string)
	GetIndexes(tb string) (idxs []string)
	CreateTable(tb string, cols []Column) error
	AlterTable(tb string, cols []Column) error
	OnConflictUpdate(cols []string) string
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
