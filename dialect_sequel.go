package goloquent

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
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

	s.db.QueryRow("SELECT DATABASE();").Scan(&name)
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

func (s *sequel) SplitJSON(name string) string {
	paths := strings.SplitN(name, ">", 2)
	if len(paths) <= 1 {
		return s.Quote(paths[0])
	}
	return fmt.Sprintf("%s->>%q",
		s.Quote(strings.TrimSpace(paths[0])),
		fmt.Sprintf("$.%s", strings.TrimSpace(paths[1])))
}

func (s sequel) JSONMarshal(v interface{}) (b json.RawMessage) {
	switch vi := v.(type) {
	case json.RawMessage:
		return vi
	case nil:
		b = json.RawMessage("null")
	case string:
		b = json.RawMessage(fmt.Sprintf("%q", vi))
	default:
		b = json.RawMessage(fmt.Sprintf("%v", vi))
	}
	return
}

func (s sequel) FilterJSON(f Filter) (string, []interface{}, error) {
	vv, err := f.Interface()
	if err != nil {
		return "", nil, err
	}
	if vv == nil {
		vv = json.RawMessage("null")
	}
	name := s.SplitJSON(f.Field())
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	switch f.operator {
	case Equal:
		buf.WriteString(fmt.Sprintf("(%s) = %s", name, variable))
	case NotEqual:
		buf.WriteString(fmt.Sprintf("(%s) <> %s", name, variable))
	case GreaterThan:
		buf.WriteString(fmt.Sprintf("(%s) > %s", name, variable))
	case GreaterEqual:
		buf.WriteString(fmt.Sprintf("(%s) >= %s", name, variable))
	case In:
		x, isOk := vv.([]interface{})
		if !isOk {
			x = append(x, vv)
		}
		if len(x) <= 0 {
			return "", nil, fmt.Errorf(`goloquent: value for "In" operator cannot be empty`)
		}
		buf.WriteString("(")
		for i := 0; i < len(x); i++ {
			buf.WriteString(fmt.Sprintf("JSON_CONTAINS(%s, %s) OR ", name, variable))
			args = append(args, s.JSONMarshal(x[i]))
		}
		buf.Truncate(buf.Len() - 4)
		buf.WriteString(")")
		return buf.String(), args, nil
	case NotIn:
		x, isOk := vv.([]interface{})
		if !isOk {
			x = append(x, vv)
		}
		if len(x) <= 0 {
			return "", nil, fmt.Errorf(`goloquent: value for "NotIn" operator cannot be empty`)
		}
		buf.WriteString("(")
		for i := 0; i < len(x); i++ {
			buf.WriteString(fmt.Sprintf("%s <> %s AND ", name, variable))
			args = append(args, s.JSONMarshal(x[i]))
		}
		buf.Truncate(buf.Len() - 4)
		buf.WriteString(")")
		return buf.String(), args, nil
	case ContainAny:
		x, isOk := vv.([]interface{})
		if !isOk {
			x = append(x, vv)
		}
		if len(x) <= 0 {
			return "", nil, fmt.Errorf(`goloquent: value for "ContainAny" operator cannot be empty`)
		}
		buf.WriteString("(")
		for i := 0; i < len(x); i++ {
			buf.WriteString(fmt.Sprintf("JSON_CONTAINS(%s, %s) OR ", name, variable))
			args = append(args, s.JSONMarshal(x[i]))
		}
		buf.Truncate(buf.Len() - 4)
		buf.WriteString(")")
		return buf.String(), args, nil
	case IsType:
		buf.WriteString(fmt.Sprintf("JSON_TYPE(%s) = UPPER(%s)", name, variable))
	case IsObject:
		vv = "OBJECT"
		buf.WriteString(fmt.Sprintf("JSON_TYPE(%s) = %s", name, variable))
	case IsArray:
		vv = "ARRAY"
		buf.WriteString(fmt.Sprintf("JSON_TYPE(%s) = %s", name, variable))
	default:
		return "", nil, fmt.Errorf("unsupported operator")
	}

	args = append(args, vv)
	return buf.String(), args, nil
}

func (s *sequel) Value(it interface{}) string {
	var str string
	switch vi := it.(type) {
	case nil:
		str = "NULL"
	case json.RawMessage:
		str = fmt.Sprintf("%q", vi)
	case string, []byte:
		str = fmt.Sprintf("%q", vi)
	case float32:
		str = strconv.FormatFloat(float64(vi), 'f', -1, 64)
	case float64:
		str = strconv.FormatFloat(vi, 'f', -1, 64)
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
	case typeOfDate:
		// sc.DefaultValue = time.Time{}
		sc.DataType = "date"
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
		case reflect.Int:
			sc.DefaultValue = int(0)
			sc.DataType = "int"
		case reflect.Int8:
			sc.DefaultValue = int8(0)
			sc.DataType = "tinyint"
		case reflect.Int16:
			sc.DefaultValue = int16(0)
			sc.DataType = "smallint"
		case reflect.Int32:
			sc.DefaultValue = int32(0)
			sc.DataType = "mediumint"
		case reflect.Int64:
			sc.DefaultValue = int64(0)
			sc.DataType = "bigint"
		case reflect.Uint:
			sc.DefaultValue = uint(0)
			sc.DataType = "int"
			sc.IsUnsigned = true
		case reflect.Uint8:
			sc.DefaultValue = uint8(0)
			sc.DataType = "tinyint"
			sc.IsUnsigned = true
		case reflect.Uint16:
			sc.DefaultValue = uint16(0)
			sc.DataType = "smallint"
			sc.IsUnsigned = true
		case reflect.Uint32:
			sc.DefaultValue = uint32(0)
			sc.DataType = "mediumint"
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
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", s.CurrentDB(), table).Scan(&count)
	return count > 0
}

func (s *sequel) HasIndex(table, idx string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?", s.CurrentDB(), table, idx).Scan(&count)
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

func (s sequel) UpdateWithLimit() bool {
	return false
}
