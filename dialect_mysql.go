package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"time"
)

type mysql struct {
	sequel
}

const minVersion = "5.7"

var _ Dialect = new(mysql)

func init() {
	RegisterDialect("mysql", new(mysql))
}

// Open :
func (s *mysql) Open(conf Config) (*sql.DB, error) {
	conf.trimSpace()
	addr, buf := "@", new(bytes.Buffer)
	buf.WriteString(conf.Username + ":" + conf.Password)
	if conf.UnixSocket != "" {
		addr += fmt.Sprintf("unix(%s)", conf.UnixSocket)
	} else {
		if conf.Host != "" && conf.Port != "" {
			addr += fmt.Sprintf("tcp(%s:%s)", conf.Host, conf.Port)
		}
	}
	buf.WriteString(addr)
	buf.WriteString(fmt.Sprintf("/%s", conf.Database))
	buf.WriteString("?parseTime=true")
	fmt.Println("Connection String :: ", buf.String())
	client, err := sql.Open("mysql", buf.String())
	if err != nil {
		return nil, err
	}
	var version string
	client.QueryRow("SELECT VERSION();").Scan(&version)
	if version < minVersion {
		return nil, fmt.Errorf("require at least %s version of mysql", minVersion)
	}
	return client, nil
}

// Quote :
func (s mysql) Quote(n string) string {
	return fmt.Sprintf("`%s`", n)
}

// Bind :
func (s mysql) Bind(uint) string {
	return "?"
}

// DataType :
func (s mysql) DataType(sc Schema) string {
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
		if t != reflect.TypeOf(OmitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", s.ToString(sc.DefaultValue)))
		}
	}
	return buf.String()
}

func (s mysql) OnConflictUpdate(table string, cols []string) string {
	buf := new(bytes.Buffer)
	buf.WriteString("ON DUPLICATE KEY UPDATE ")
	for _, c := range cols {
		buf.WriteString(fmt.Sprintf("%s=VALUES(%s),", s.Quote(c), s.Quote(c)))
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

func (s mysql) CreateTable(table string, columns []Column) error {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", s.GetTable(table)))
	for _, c := range columns {
		for _, ss := range s.GetSchema(c) {
			buf.WriteString(fmt.Sprintf("%s %s,", s.Quote(ss.Name), s.DataType(ss)))
			if ss.IsIndexed {
				idx := fmt.Sprintf("%s_%s_%s", table, ss.Name, "Idx")
				buf.WriteString(fmt.Sprintf("INDEX %s (%s),", s.Quote(idx), s.Quote(ss.Name)))
			}
		}
	}
	buf.WriteString(fmt.Sprintf("PRIMARY KEY (%s)", s.Quote(pkColumn)))
	buf.WriteString(fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s;",
		utf8CharSet.Encoding, utf8CharSet.Collation))
	log.Println(buf.String())
	if _, err := s.db.Exec(buf.String()); err != nil {
		return err
	}
	return nil
}

func (s *mysql) AlterTable(table string, columns []Column) error {
	cols := newDictionary(s.GetColumns(table))
	idxs := newDictionary(s.GetIndexes(table))

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s", s.GetTable(table)))
	suffix := "FIRST"
	for _, c := range columns {
		for _, ss := range s.GetSchema(c) {
			action := "ADD"
			if cols.has(ss.Name) {
				action = "MODIFY"
			}
			buf.WriteString(fmt.Sprintf("%s %s %s %s,",
				action, s.Quote(ss.Name), s.DataType(ss), suffix))
			suffix = fmt.Sprintf("AFTER %s", s.Quote(ss.Name))

			if ss.IsIndexed {
				idx := fmt.Sprintf("%s_%s_%s", table, ss.Name, "idx")
				if idxs.has(idx) {
					idxs.delete(idx)
				} else {
					buf.WriteString(fmt.Sprintf(" ADD INDEX %s (%s),",
						s.Quote(idx), s.Quote(ss.Name)))
				}
			}
			cols.delete(ss.Name)
		}
	}

	for _, col := range cols.keys() {
		buf.WriteString(fmt.Sprintf(" DROP COLUMN %s,", s.Quote(col)))
	}

	for _, idx := range idxs.keys() {
		buf.WriteString(fmt.Sprintf(
			" DROP INDEX %s,", s.Quote(idx)))
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(";")
	log.Println(buf.String())
	if _, err := s.db.Exec(buf.String()); err != nil {
		return err
	}
	return nil
}

func (s mysql) ToString(it interface{}) string {
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
		v = fmt.Sprintf(`"%s"`, vi.Format("2006-01-02 15:04:05"))
	case []interface{}:
		v = fmt.Sprintf(`"%s"`, "[]")
	case map[string]interface{}:
		v = fmt.Sprintf(`"%s"`, "{}")
	case nil:
		v = "NULL"
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}
