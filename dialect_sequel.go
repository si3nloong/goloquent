package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func checkMultiPtr(v reflect.Value) (isPtr bool, t reflect.Type) {
	t = v.Type().Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		isPtr = true
	}
	return
}

type sqlCommon interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type sqlExtra interface {
	sqlCommon
	Log()
}

// sequel :
type sequel struct {
	dbName string
	db     Client
}

var _ Dialect = new(sequel)

func init() {
	RegisterDialect("common", new(sequel))
}

// SetDB :
func (s *sequel) SetDB(db Client) {
	s.db = db
}

func (s *sequel) Open(conf Config) (*sql.DB, error) {
	connStr := conf.Username + ":" + conf.Password + "@/" + conf.Database
	client, err := sql.Open("common", connStr)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// CreateIndex :
func (s *sequel) CreateIndex(idx string, cols []string) string {
	return fmt.Sprintf("CREATE INDEX %s (%s)",
		s.Quote(idx),
		s.Quote(strings.Join(cols, ",")))
}

// GetTable :
func (s *sequel) GetTable(name string) string {
	return fmt.Sprintf("%s.%s", s.Quote(s.dbName), s.Quote(name))
}

// Version :
func (s *sequel) Version() (version string) {
	s.db.QueryRow("SELECT VERSION();").Scan(&version)
	return
}

// CurrentDB :
func (s *sequel) CurrentDB() (name string) {
	if s.dbName != "" {
		name = s.dbName
		return
	}

	s.db.QueryRow("SELECT DATABASE()").Scan(&name)
	s.dbName = name
	return
}

// Quote :
func (s *sequel) Quote(n string) string {
	return fmt.Sprintf("`%s`", n)
}

// Bind :
func (s *sequel) Bind(uint) string {
	return "?"
}

func (s *sequel) Value(it interface{}) string {
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

// DataType :
func (s *sequel) DataType(sc Schema) string {
	buf := new(bytes.Buffer)
	buf.WriteString(sc.DataType)
	if sc.IsUnsigned {
		buf.WriteString(" UNSIGNED")
	}
	if sc.CharSet.Encoding != "" && sc.CharSet.Collation != "" {
		buf.WriteString(fmt.Sprintf(" CHARACTER SET %s COLLATE %s",
			s.Quote(sc.CharSet.Encoding),
			s.Quote(sc.CharSet.Collation)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		if !sc.IsOmitEmpty() {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", s.ToString(sc.DefaultValue)))
		}
	}
	return buf.String()
}

func (s *sequel) ToString(it interface{}) string {
	var v string
	switch vi := it.(type) {
	case string:
		v = fmt.Sprintf(`'%s'`, vi)
	case bool:
		v = fmt.Sprintf("%t", vi)
	case uint, uint8, uint16, uint32, uint64:
		v = fmt.Sprintf("%d", vi)
	case int, int8, int16, int32, int64:
		v = fmt.Sprintf("%d", vi)
	case float32, float64:
		v = fmt.Sprintf("%v", vi)
	case time.Time:
		v = fmt.Sprintf(`'%s'`, vi.Format("2006-01-02 15:04:05"))
	case []interface{}:
		v = fmt.Sprintf(`'%s'`, "[]")
	case nil:
		v = "NULL"
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}

// GetSchema :
func (s *sequel) GetSchema(c Column) []Schema {
	f := c.field
	root := f.getRoot()
	t := root.typeOf
	if root.isFlatten() {
		if !root.isSlice() {
			t = f.typeOf
		}
	}

	sc := Schema{
		Name:       c.Name(),
		IsNullable: f.isPtrChild,
		IsIndexed:  f.IsIndex(),
	}
	if t.Kind() == reflect.Ptr {
		sc.IsNullable = true
		if t == typeOfPtrKey {
			sc.IsIndexed = true
			sc.DataType = fmt.Sprintf("varchar(%d)", pkLen)
			sc.CharSet = latin1CharSet
			if f.name == keyFieldName {
				sc.Name = pkColumn
				sc.DefaultValue = OmitDefault(nil)
				sc.IsIndexed = false
			}
			return []Schema{sc}
		}
		t = t.Elem()
	}

	switch t {
	case typeOfByte:
		sc.DefaultValue = OmitDefault(nil)
		sc.DataType = "mediumblob"
	case typeOfTime:
		sc.DefaultValue = time.Time{}
		sc.DataType = "datetime"
	case typeOfSoftDelete:
		sc.DefaultValue = OmitDefault(nil)
		sc.IsNullable = true
		sc.IsIndexed = true
		sc.DataType = "datetime"
	default:
		switch t.Kind() {
		case reflect.String:
			sc.DefaultValue = ""
			sc.DataType = fmt.Sprintf("varchar(%d)", 191)
			if f.IsLongText() {
				sc.DefaultValue = nil
				sc.DataType = "text"
			}
			if f.Get("datatype") != "" {
				sc.DataType = f.Get("datatype")
			}
			sc.CharSet = utf8mb4CharSet
			charset := f.Get("charset")
			if charset != "" {
				sc.CharSet.Encoding = charset
				sc.CharSet.Collation = fmt.Sprintf("%s_general_ci", charset)
				if f.Get("collate") != "" {
					sc.CharSet.Collation = f.Get("collate")
				}
			}
		case reflect.Bool:
			sc.DefaultValue = false
			sc.DataType = "boolean"
		case reflect.Int8:
			sc.DefaultValue = int8(0)
			sc.DataType = "tinyint"
		case reflect.Int16:
			sc.DefaultValue = int16(0)
			sc.DataType = "mediumint"
		case reflect.Int, reflect.Int32:
			sc.DefaultValue = int(0)
			sc.DataType = "int"
		case reflect.Int64:
			sc.DefaultValue = int64(0)
			sc.DataType = "bigint"
		case reflect.Uint8:
			sc.DefaultValue = uint8(0)
			sc.DataType = "tinyint"
			sc.IsUnsigned = true
		case reflect.Uint, reflect.Uint32:
			sc.DefaultValue = uint(0)
			sc.DataType = "int"
			sc.IsUnsigned = true
		case reflect.Uint64:
			sc.DefaultValue = uint64(0)
			sc.DataType = "bigint"
			sc.IsUnsigned = true
		case reflect.Float32, reflect.Float64:
			sc.DefaultValue = float64(0)
			sc.DataType = "double"
			sc.IsUnsigned = f.IsUnsigned()
		case reflect.Slice, reflect.Array:
			sc.DefaultValue = OmitDefault(nil)
			sc.DataType = "json"
		default:
			sc.DefaultValue = OmitDefault(nil)
			sc.DataType = "json"
		}
	}

	return []Schema{sc}
}

// GetColumns :
func (s *sequel) GetColumns(table string) (columns []string) {
	stmt := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;"
	rows, _ := s.db.Query(stmt, s.CurrentDB(), table)
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		columns = append(columns, "")
		rows.Scan(&columns[i])
	}
	return
}

// GetIndexes :
func (s *sequel) GetIndexes(table string) (idxs []string) {
	stmt := "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME <> ?;"
	rows, _ := s.db.Query(stmt, s.CurrentDB(), table, "PRIMARY")
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		idxs = append(idxs, "")
		rows.Scan(&idxs[i])
	}
	return
}

func (s *sequel) HasTable(table string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?", s.CurrentDB(), table).Scan(&count)
	return count > 0
}

// OnConflictUpdate :
func (s *sequel) OnConflictUpdate(table string, cols []string) string {
	buf := new(bytes.Buffer)
	buf.WriteString("ON DUPLICATE KEY UPDATE ")
	for _, c := range cols {
		buf.WriteString(fmt.Sprintf("%s=VALUES(%s),", s.Quote(c), s.Quote(c)))
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

func (s *sequel) CreateTable(string, []Column) error {
	return nil
}

func (s *sequel) AlterTable(string, []Column) error {
	return nil
}
