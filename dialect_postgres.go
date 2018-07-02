package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
)

type postgres struct {
	sequel
}

var _ Dialect = new(postgres)

func init() {
	RegisterDialect("postgres", new(postgres))
}

// Open :
func (p *postgres) Open(conf Config) (*sql.DB, error) {
	addr, buf := "@", new(bytes.Buffer)
	buf.WriteString("postgres://")
	buf.WriteString(conf.Username + ":" + conf.Password)
	if conf.UnixSocket != "" {
		addr += fmt.Sprintf("unix(%s)", conf.UnixSocket)
	} else {
		if conf.Host != "" && conf.Port != "" &&
			conf.Host != "localhost" && conf.Host != "127.0.0.1" {
			addr += fmt.Sprintf("tcp(%s:%s)", conf.Host, conf.Port)
		}
	}
	buf.WriteString(addr)
	buf.WriteString(fmt.Sprintf("/%s", conf.Database))
	buf.WriteString("?sslmode=disable")
	log.Println("Connection String :", buf.String())
	client, err := sql.Open("postgres", buf.String())
	if err != nil {
		return nil, err
	}
	return client, nil
}

// GetTable :
func (p postgres) GetTable(name string) string {
	return p.Quote(name)
}

// CurrentDB :
func (p *postgres) CurrentDB() (name string) {
	if p.dbName != "" {
		name = p.dbName
		return
	}

	p.db.QueryRow("SELECT current_database();").Scan(&name)
	p.dbName = name
	return
}

// CreateIndex :
func (p postgres) CreateIndex(idx string, cols []string) string {
	return fmt.Sprintf("CREATE INDEX %s (%s)", p.Quote(idx), p.Quote(strings.Join(cols, ",")))
}

func (p postgres) Quote(n string) string {
	return fmt.Sprintf(`"%s"`, n)
}

func (p postgres) Bind(i uint) string {
	return fmt.Sprintf("$%d", i)
}

func (p postgres) escapeQuote(v string) string {
	return strings.Replace(v, `'`, `''`, -1)
}

func (p postgres) Value(it interface{}) string {
	var str string
	switch vi := it.(type) {
	case nil:
		str = "NULL"
	case string, []byte:
		str = fmt.Sprintf(`'%s'`, p.escapeQuote(fmt.Sprintf(`%s`, vi)))
	default:
		str = fmt.Sprintf("%v", vi)
	}
	return str
}

// DataType :
func (p postgres) DataType(sc Schema) string {
	buf := new(bytes.Buffer)
	buf.WriteString(sc.DataType)
	if sc.IsUnsigned {
		buf.WriteString(fmt.Sprintf(" CHECK (%s > 0)", p.Quote(sc.Name)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		t := reflect.TypeOf(sc.DefaultValue)
		if t != reflect.TypeOf(OmitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", p.ToString(sc.DefaultValue)))
		}
	}
	// if reflect.DeepEqual(sc.CharSet, latin1CharSet) {
	// 	buf.WriteString(fmt.Sprintf(" COLLATE %q", "C"))
	// }
	return buf.String()
}

func (p postgres) OnConflictUpdate(table string, cols []string) string {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET ", p.Quote(pkColumn)))
	for _, c := range cols {
		buf.WriteString(fmt.Sprintf("%s = %s.%s,", p.Quote(c), p.GetTable(table), p.Quote(c)))
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

func (p postgres) GetSchema(c Column) []Schema {
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
	}

	if t.Kind() == reflect.Ptr {
		sc.IsNullable = true
		if t == typeOfPtrKey {
			if f.name == keyFieldName {
				return []Schema{
					Schema{pkColumn, fmt.Sprintf("varchar(%d)", pkLen), OmitDefault(nil), false, false, false, latin1CharSet},
				}
			}
			sc.IsIndexed = true
			sc.DataType = fmt.Sprintf("varchar(%d)", pkLen)
			sc.CharSet = latin1CharSet
			return []Schema{sc}
		}
		t = t.Elem()
	}

	switch t {
	case typeOfByte:
		sc.DefaultValue = OmitDefault(nil)
		sc.DataType = "bytea"
	case typeOfTime:
		sc.DefaultValue = time.Time{}
		sc.DataType = "timestamp"
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
			sc.CharSet = utf8CharSet
			charset := f.Get("charset")
			if charset != "" {
				sc.CharSet.Encoding = charset
			}
		case reflect.Bool:
			sc.DefaultValue = false
			sc.DataType = "bool"
		case reflect.Int8:
			sc.DefaultValue = int8(0)
			sc.DataType = "smallint"
		case reflect.Int16:
			sc.DefaultValue = int16(0)
			sc.DataType = "integer"
		case reflect.Int, reflect.Int32:
			sc.DefaultValue = int32(0)
			sc.DataType = "integer"
		case reflect.Int64:
			sc.DefaultValue = int64(0)
			sc.DataType = "bigint"
		case reflect.Uint8:
			sc.DefaultValue = uint8(0)
			sc.IsUnsigned = true
			sc.DataType = "smallint"
		case reflect.Uint16:
			sc.DefaultValue = uint16(0)
			sc.IsUnsigned = true
			sc.DataType = "integer"
		case reflect.Uint, reflect.Uint32:
			sc.DefaultValue = uint32(0)
			sc.IsUnsigned = true
			sc.DataType = "integer"
		case reflect.Uint64:
			sc.DefaultValue = uint64(0)
			sc.IsUnsigned = true
			sc.DataType = "bigint"
		case reflect.Float32, reflect.Float64:
			sc.DefaultValue = float64(0)
			sc.DataType = "real"
		default:
			sc.DataType = "jsonb"
			sc.CharSet = utf8CharSet
		}
	}

	return []Schema{sc}
}

// GetColumns :
func (p *postgres) GetColumns(table string) (columns []string) {
	stmt := "SELECT * FROM INFORMATION_SCHEMA.columns WHERE table_schema = CURRENT_SCHEMA() AND table_name = $1;"
	rows, _ := p.db.Query(stmt, table)
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		columns = append(columns, "")
		rows.Scan(&columns[i])
	}
	return
}

// GetIndexes :
func (p *postgres) GetIndexes(table string) (idxs []string) {
	stmt := "SELECT indexname FROM pg_indexes WHERE schemaname = CURRENT_SCHEMA() AND tablename = $1;"
	rows, _ := p.db.Query(stmt, table)
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		idxs = append(idxs, "")
		rows.Scan(&idxs[i])
	}
	return
}

func (p *postgres) HasTable(table string) bool {
	var count int
	p.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE table_type = 'BASE TABLE' AND table_schema = CURRENT_SCHEMA() AND table_name = $1;", table).Scan(&count)
	return count > 0
}

func (p *postgres) ToString(it interface{}) string {
	var v string
	switch vi := it.(type) {
	case nil:
		v = "NULL"
	case string:
		v = fmt.Sprintf(`'%s'`, "")
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
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}

func (p *postgres) CreateTable(table string, columns []Column) error {
	idxs := make([]string, 0, len(columns))
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", p.GetTable(table)))
	for _, c := range columns {
		for _, ss := range p.GetSchema(c) {
			buf.WriteString(fmt.Sprintf("%s %s,",
				p.Quote(ss.Name),
				p.DataType(ss)))

			if ss.IsIndexed {
				idx := fmt.Sprintf("%s_%s_%s", table, ss.Name, "Idx")
				stmt := fmt.Sprintf("CREATE INDEX %s ON %s (%s);",
					p.Quote(idx), p.GetTable(table), p.Quote(ss.Name))
				idxs = append(idxs, stmt)
			}
		}
	}
	buf.WriteString(fmt.Sprintf("PRIMARY KEY (%s)", p.Quote(pkColumn)))
	buf.WriteString(");")
	if err := p.db.execStmt(&stmt{
		statement: buf,
	}); err != nil {
		return err
	}

	log.Println(idxs)
	for _, idx := range idxs {
		if _, err := p.db.Exec(idx); err != nil {
			return err
		}
	}

	return nil
}

func (p *postgres) AlterTable(table string, columns []Column) error {
	cols := newDictionary(p.GetColumns(table))
	idxs := newDictionary(p.GetIndexes(table))
	idxs.delete(fmt.Sprintf("%s_pkey", table))
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s ", p.GetTable(table)))
	for _, c := range columns {
		for _, ss := range p.GetSchema(c) {
			prefix := fmt.Sprintf("ALTER COLUMN %s", p.Quote(ss.Name))
			buf.WriteString(fmt.Sprintf("%s TYPE %s", prefix, ss.DataType))
			buf.WriteString(",")
			if !ss.IsNullable {
				buf.WriteString(prefix + " SET NOT NULL,")
				if !ss.IsOmitEmpty() {
					buf.WriteString(fmt.Sprintf("%s SET DEFAULT %s,",
						prefix, p.ToString(ss.DefaultValue)))
				}
			}
			if ss.IsIndexed {
				idx := fmt.Sprintf("%s_%s_%s", table, ss.Name, "idx")
				if idxs.has(idx) {
					idxs.delete(idx)
				} else {

					// buf.WriteString(fmt.Sprintf(
					// 	" CREATE INDEX %s ON (%s);",
					// 	p.Quote(idx),
					// 	p.Quote(ss.Name)))
				}
			}
			cols.delete(ss.Name)
		}
	}

	for _, col := range cols.keys() {
		buf.WriteString(fmt.Sprintf(" DROP COLUMN %s,", p.Quote(col)))
	}

	buf.Truncate(buf.Len() - 1)
	buf.WriteString(";")

	log.Println(idxs.keys())
	return p.db.execStmt(&stmt{
		statement: buf,
	})

	// for _, idx := range idxs.keys() {
	// 	buff := new(bytes.Buffer)
	// 	buff.WriteString(fmt.Sprintf("DROP INDEX %s;", p.Quote(idx)))
	// 	p.db.ConsoleLog(&Stmt{buff, nil, nil})
	// 	if _, err := tx.Exec(buff.String()); err != nil {
	// 		return err
	// 	}
	// }

	// for _, idx := range idxs.keys() {
	// 	stmt := fmt.Sprintf("CREATE INDEX %s ON %s ();", p.Quote(idx), p.Quote(table))
	// 	if _, err := tx.Exec(stmt); err != nil {
	// 		return err
	// 	}
	// }
}
