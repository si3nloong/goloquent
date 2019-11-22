package goloquent

import (
	"database/sql"
	"encoding/json"
)

// Supported storage operator
const (
	MYSQL      = "Mysql"
	POSTGRESQL = "Postgres"
)

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
	AlterTable(tb string, cols []Column, unsafe bool) error
	OnConflictUpdate(tb string, cols []string) string
	UpdateWithLimit() bool
	ReplaceInto(src, dst string) error
}

// MatchStorageOperator : Will match your user defined storage and create
// a corresponding storage operator. If not found, return false result.
func MatchStorageOperator(driver string) (Dialect, bool) {
	switch driver {
	case MYSQL:
		return new(mysql), true
	case POSTGRESQL:
		return new(postgres), true
	default:
		return nil, false
	}
}
