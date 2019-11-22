package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/si3nloong/goloquent/types"
)

type mysql struct {
	sequel
}

const minVersion = "5.7"

// var _ Dialect = new(mysql)

// func init() {
// 	RegisterDialect("mysql", new(mysql))
// }

// Open :
func (s *mysql) Open(conf Config) (*sql.DB, error) {
	addr, buf := "@", new(bytes.Buffer)
	buf.WriteString(conf.Username + ":" + conf.Password)
	if conf.UnixSocket != "" {
		addr += fmt.Sprintf("unix(%s)", conf.UnixSocket)
	} else {
		host, port := "localhost", "3306"
		if conf.Host != "" {
			host = conf.Host
		}
		if conf.Port != "" {
			port = conf.Port
		}
		addr += fmt.Sprintf("tcp(%s:%s)", host, port)
	}
	buf.WriteString(addr)
	buf.WriteString(fmt.Sprintf("/%s", conf.Database))
	buf.WriteString("?parseTime=true")
	buf.WriteString("&charset=utf8mb4&collation=utf8mb4_unicode_ci")
	log.Println("Connection String :", buf.String())
	client, err := sql.Open("mysql", buf.String())
	if err != nil {
		return nil, err
	}
	client.SetMaxOpenConns(300)
	return client, nil
}

// Version :
func (s mysql) Version() (version string) {
	verRgx := regexp.MustCompile(`(\d\.\d)`)
	s.db.QueryRow("SELECT VERSION();").Scan(&version)
	log.Println("MySQL version :", version)
	if compareVersion(verRgx.FindStringSubmatch(version)[0], minVersion) > 0 {
		panic(fmt.Errorf("require at least %s version of mysql", minVersion))
	}
	return
}

// Quote :
func (s mysql) Quote(n string) string {
	return "`" + n + "`"
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
	if sc.CharSet.Encoding != "" && sc.CharSet.Collation != "" {
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
			if ss.IsIndexed || c.field.typeOf == typeOfSoftDelete {
				idx := fmt.Sprintf("%s_%s_%s", table, ss.Name, "idx")
				buf.WriteString(fmt.Sprintf("INDEX %s (%s),", s.Quote(idx), s.Quote(ss.Name)))
			}
		}
	}
	buf.WriteString(fmt.Sprintf("PRIMARY KEY (%s)", s.Quote(pkColumn)))
	buf.WriteString(fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s;",
		s.Quote(s.db.CharSet.Encoding), s.Quote(s.db.CharSet.Collation)))
	return s.db.execStmt(&stmt{statement: buf})
}

func (s *mysql) AlterTable(table string, columns []Column, unsafe bool) error {
	cols := types.StringSlice(s.GetColumns(table))
	idxs := types.StringSlice(s.GetIndexes(table))

	var idx string
	blr := new(bytes.Buffer)
	blr.WriteString(`ALTER TABLE ` + s.GetTable(table) + ` `)
	suffix := "FIRST"
	for _, c := range columns {
		for _, ss := range s.GetSchema(c) {
			if cols.IndexOf(ss.Name) > -1 {
				blr.WriteString(`MODIFY`)
			} else {
				blr.WriteString(`ADD`)
			}

			blr.WriteString(` ` + s.Quote(ss.Name) + ` `)
			blr.WriteString(s.DataType(ss) + ` ` + suffix)
			suffix = `AFTER ` + s.Quote(ss.Name)

			if ss.IsIndexed || c.field.typeOf == typeOfSoftDelete {
				idx = table + `_` + ss.Name + `_idx`
				if idxs.IndexOf(idx) < 0 {
					blr.WriteRune(',')
					blr.WriteString(`ADD INDEX ` + s.Quote(idx))
					blr.WriteString(` (` + s.Quote(ss.Name) + `)`)
				}
			}
			blr.WriteRune(',')
		}
	}

	// for _, col := range cols.keys() {
	// 	blr.WriteString(fmt.Sprintf("DROP COLUMN %s,", s.Quote(col)))
	// }
	// for _, idx := range idxs.keys() {
	// 	buf.WriteString(fmt.Sprintf("DROP INDEX %s,", s.Quote(idx)))
	// }

	blr.WriteString(` CHARACTER SET ` + s.Quote(s.db.CharSet.Encoding))
	blr.WriteString(` COLLATE ` + s.Quote(s.db.CharSet.Collation))
	blr.WriteRune(';')
	return s.db.execStmt(&stmt{statement: blr})
}

func (s mysql) ToString(it interface{}) string {
	var v string
	switch vi := it.(type) {
	case string:
		v = fmt.Sprintf("%q", vi)
	case bool:
		v = fmt.Sprintf("%t", vi)
	case uint, uint8, uint16, uint32, uint64:
		v = fmt.Sprintf("%d", vi)
	case int, int8, int16, int32, int64:
		v = fmt.Sprintf("%d", vi)
	case float32:
		v = strconv.FormatFloat(float64(vi), 'f', -1, 64)
	case float64:
		v = strconv.FormatFloat(vi, 'f', -1, 64)
	case time.Time:
		v = fmt.Sprintf(`"%s"`, vi.Format("2006-01-02 15:04:05"))
	// case json.RawMessage:
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

func (s mysql) UpdateWithLimit() bool {
	return true
}

func (s mysql) ReplaceInto(src, dst string) error {
	src, dst = s.GetTable(src), s.GetTable(dst)
	buf := new(bytes.Buffer)
	buf.WriteString("REPLACE INTO ")
	buf.WriteString(dst + " ")
	buf.WriteString("SELECT * FROM ")
	buf.WriteString(src)
	buf.WriteString(";")
	return s.db.execStmt(&stmt{
		statement: buf,
	})
}
