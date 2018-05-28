package goloquent

import (
	"database/sql"
	"reflect"
)

// Dialect :
type Dialect interface {
	SetDB(db sqlCommon)
	GetTable(ns string) string
	Open(c Config) (*sql.DB, error)
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
	OnConflictUpdate(cols []string) string
	// AlterColumn(s Schema) string
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
