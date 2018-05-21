package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

type mysql struct {
	sequel
}

func init() {
	RegisterDialect("mysql", new(mysql))
}

// Open :
func (s *mysql) Open(conf Config) (*sql.DB, error) {
	connStr := conf.Username + ":" + conf.Password + "@/" + conf.Database
	client, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Quote :
func (s *mysql) Quote(n string) string {
	return fmt.Sprintf("`%s`", n)
}

// Bind :
func (s *mysql) Bind(int) string {
	return "?"
}

// DataType :
func (s *mysql) DataType(sc Schema) string {
	buf := new(bytes.Buffer)
	buf.WriteString(sc.DataType)
	if sc.IsUnsigned {
		buf.WriteString(" UNSIGNED")
	}
	if sc.CharSet != nil {
		buf.WriteString(fmt.Sprintf(" CHARACTER SET %s COLLATE %s",
			s.Quote(sc.CharSet.Encoding),
			s.Quote(sc.CharSet.Collation)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		t := reflect.TypeOf(sc.DefaultValue)
		if t != reflect.TypeOf(omitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", s.toString(sc.DefaultValue)))
		}
	}
	return buf.String()
}

func (s *mysql) OnConflictUpdate(cols []string) string {
	buf := new(bytes.Buffer)
	buf.WriteString("ON DUPLICATE KEY UPDATE ")
	for _, c := range cols {
		if c == keyColumn || c == parentColumn {
			continue
		}
		buf.WriteString(fmt.Sprintf("%s=values(%s),",
			s.Quote(c), s.Quote(c)))
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

func (s *mysql) toString(it interface{}) string {
	var v string
	switch vi := it.(type) {
	case string:
		v = fmt.Sprintf("%q", "")
	case bool:
		v = fmt.Sprintf("%t", vi)
	case uint, uint8, uint16, uint32, uint64:
		v = fmt.Sprintf("%d", vi)
	case int, int8, int16, int32, int64:
		v = fmt.Sprintf("%d", vi)
	case float32, float64:
		v = fmt.Sprintf("%v", vi)
	case time.Time:
		v = fmt.Sprintf("%q", vi.Format("2006-01-02 15:04:05"))
	case []interface{}:
		v = fmt.Sprintf("%q", "[]")
	case nil:
		v = "NULL"
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}
