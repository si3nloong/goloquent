package goloquent

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
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

func (p postgres) escapeSingleQuote(n string) string {
	return strings.Replace(n, `'`, `\'`, -1)
}

// Open :
func (p *postgres) Open(conf Config) (*sql.DB, error) {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("user='%s' ", p.escapeSingleQuote(conf.Username)))
	buf.WriteString(fmt.Sprintf("password='%s' ", p.escapeSingleQuote(conf.Password)))
	if conf.UnixSocket != "" {
		buf.WriteString(fmt.Sprintf("host=/%s ", strings.Trim(conf.UnixSocket, `/`)))
	} else {
		host, port := "localhost", "5432"
		if conf.Host != "" {
			host = conf.Host
		}
		if conf.Port != "" {
			port = conf.Port
		}
		buf.WriteString(fmt.Sprintf("host=%s port=%s ", host, port))
	}
	buf.WriteString(fmt.Sprintf("dbname='%s' ", p.escapeSingleQuote(conf.Database)))
	buf.WriteString("sslmode=disable")

	log.Println("Connection String :", buf.String()) // should be DSN string
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

func (p postgres) Quote(n string) string {
	return strconv.Quote(n)
}

func (p postgres) Bind(i uint) string {
	return `$` + strconv.FormatUint(uint64(i), 64)
}

func (p postgres) SplitJSON(name string) string {
	paths := strings.SplitN(name, ">", 2)
	if len(paths) <= 1 {
		return p.Quote(paths[0])
	}
	vv := strings.Split(strings.TrimSpace(paths[1]), `.`)
	return fmt.Sprintf(`%s->%s`,
		p.Quote(strings.TrimSpace(paths[0])),
		`'`+strings.Join(vv, p.Value(`->`))+`'`)
}

func (p postgres) JSONMarshal(v interface{}) (b json.RawMessage) {
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

func (p postgres) FilterJSON(f Filter) (string, []interface{}, error) {
	vv, err := f.Interface()
	if err != nil {
		return "", nil, err
	}
	if vv == nil {
		vv = json.RawMessage("null")
	}
	name := p.SplitJSON(f.Field())
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
			buf.WriteString(fmt.Sprintf("(%s = %s) OR ", name, variable))
			args = append(args, p.JSONMarshal(x[i]))
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
			return "", nil, fmt.Errorf(`goloquent: value for "In" operator cannot be empty`)
		}
		buf.WriteString("(")
		for i := 0; i < len(x); i++ {
			buf.WriteString(fmt.Sprintf("(%s <> %s) AND ", name, variable))
			args = append(args, p.JSONMarshal(x[i]))
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
			return "", nil, fmt.Errorf(`goloquent: value for "In" operator cannot be empty`)
		}
		buf.WriteString(fmt.Sprintf("%s ?| array[", name))
		for i := 0; i < len(x); i++ {
			buf.WriteString(variable + ",")
			args = append(args, x[i])
		}
		buf.Truncate(buf.Len() - 1)
		buf.WriteString("]")
		return buf.String(), args, nil
	case IsType:
		args = append(args, vv)
		buf.WriteString(fmt.Sprintf("jsonb_typeof((%s)::jsonb) = LOWER(%s)", name, variable))
		return buf.String(), args, nil
	case IsObject:
		vv = json.RawMessage([]byte("{}"))
		buf.WriteString(fmt.Sprintf("(%s)::jsonb @> %s::jsonb", name, variable))
	case IsArray:
		vv = json.RawMessage([]byte("[]"))
		buf.WriteString(fmt.Sprintf("(%s)::jsonb @> %s::jsonb", name, variable))
	default:
		return "", nil, fmt.Errorf("unsupported operator")
	}

	args = append(args, p.JSONMarshal(vv))
	return buf.String(), args, nil
}

func (p postgres) Value(it interface{}) string {
	var str string
	switch vi := it.(type) {
	case nil:
		str = "NULL"
	case json.RawMessage:
		str = fmt.Sprintf(`'%s'`, escapeSingleQuote(fmt.Sprintf(`%s`, vi)))
	case string, []byte:
		str = fmt.Sprintf(`'%s'`, escapeSingleQuote(fmt.Sprintf(`%s`, vi)))
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
		buf.WriteString(fmt.Sprintf(" CHECK (%s >= 0)", p.Quote(sc.Name)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		t := reflect.TypeOf(sc.DefaultValue)
		if t != reflect.TypeOf(OmitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", p.ToString(sc.DefaultValue)))
		}
	}
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
	case typeOfJSONRawMessage:
		sc.DefaultValue = OmitDefault(nil)
		sc.DataType = "jsonb"
	case typeOfByte:
		sc.DefaultValue = OmitDefault(nil)
		sc.DataType = "bytea"
	case typeOfDate:
		sc.DefaultValue = "0001-01-01"
		sc.DataType = "date"
	case typeOfTime:
		sc.DefaultValue = time.Time{}
		sc.DataType = "timestamp"
	case typeOfSoftDelete:
		sc.DefaultValue = OmitDefault(nil)
		sc.IsNullable = true
		sc.IsIndexed = true
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
		case reflect.Bool:
			sc.DefaultValue = false
			sc.DataType = "bool"
		case reflect.Int:
			sc.DefaultValue = int(0)
			sc.DataType = "integer"
		case reflect.Int8:
			sc.DefaultValue = int8(0)
			sc.DataType = "smallint"
		case reflect.Int16:
			sc.DefaultValue = int16(0)
			sc.DataType = "smallint"
		case reflect.Int32:
			sc.DefaultValue = int32(0)
			sc.DataType = "integer"
		case reflect.Int64:
			sc.DefaultValue = int64(0)
			sc.DataType = "bigint"
		case reflect.Uint:
			sc.DefaultValue = uint(0)
			sc.DataType = "integer"
			sc.IsUnsigned = true
		case reflect.Uint8:
			sc.DefaultValue = uint8(0)
			sc.DataType = "smallint"
			sc.IsUnsigned = true
		case reflect.Uint16:
			sc.DefaultValue = uint16(0)
			sc.DataType = "smallint"
			sc.IsUnsigned = true
		case reflect.Uint32:
			sc.DefaultValue = uint32(0)
			sc.DataType = "integer"
			sc.IsUnsigned = true
		case reflect.Uint64:
			sc.DefaultValue = uint64(0)
			sc.DataType = "bigint"
			sc.IsUnsigned = true
		case reflect.Float32, reflect.Float64:
			sc.DefaultValue = float64(0)
			sc.DataType = "real"
		default:
			sc.DataType = "jsonb"
		}
	}

	return []Schema{sc}
}

// GetColumns :
func (p *postgres) GetColumns(table string) (columns []string) {
	stmt := "SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE table_schema = CURRENT_SCHEMA() AND table_name = $1;"
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

func (p *postgres) HasIndex(table, idx string) bool {
	var count int
	p.db.QueryRow("SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2 AND schemaname = CURRENT_SCHEMA()", table, idx).Scan(&count)
	return count > 0
}

func (p *postgres) ToString(it interface{}) string {
	var v string
	switch vi := it.(type) {
	case nil:
		v = "NULL"
	case json.RawMessage:
		v = fmt.Sprintf(`'%s'`, vi)
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
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}

func (p *postgres) CreateTable(table string, columns []Column) error {
	idxs := make([]string, 0, len(columns))
	conn := p.db.sqlCommon.(*sql.DB)
	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
	log.Println(buf.String())
	if _, err := tx.Exec(buf.String()); err != nil {
		return err
	}

	for _, idx := range idxs {
		if _, err := tx.Exec(idx); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (p *postgres) AlterTable(table string, columns []Column) error {
	cols := newDictionary(p.GetColumns(table))
	idxs := newDictionary(p.GetIndexes(table))
	idxs.delete(fmt.Sprintf("%s_pkey", table))
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s ", p.GetTable(table)))
	for _, c := range columns {
		for _, ss := range p.GetSchema(c) {
			if !cols.has(ss.Name) {
				buf.WriteString(fmt.Sprintf("ADD COLUMN %s %s", p.Quote(ss.Name), ss.DataType))
				if !ss.IsNullable {
					buf.WriteString(" NOT NULL")
					if !ss.IsOmitEmpty() {
						buf.WriteString(fmt.Sprintf(" DEFAULT %s",
							p.ToString(ss.DefaultValue)))
					}
				}
				buf.WriteString(",")
			} else {
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

func (p *postgres) ReplaceInto(src, dst string) error {
	cols := p.GetColumns(src)
	pk := p.Quote(pkColumn)
	src, dst = p.GetTable(src), p.GetTable(dst)
	buf := new(bytes.Buffer)
	buf.WriteString("WITH patch AS (")
	buf.WriteString("UPDATE " + dst + " SET ")
	for _, c := range cols {
		if c == pkColumn {
			continue
		}
		cc := p.Quote(c)
		buf.WriteString(cc + " = " + src + "." + cc + ",")
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(" FROM " + src + " ")
	buf.WriteString("WHERE " + src + "." + pk + " = " + dst + "." + pk + " ")
	buf.WriteString("RETURNING " + src + "." + pk + ") ")
	buf.WriteString("INSERT INTO " + dst + " ")
	buf.WriteString("SELECT * FROM " + src + " ")
	buf.WriteString("WHERE NOT EXISTS ")
	buf.WriteString("(SELECT 1 FROM patch WHERE " + pk + " = " + src + "." + pk + ")")
	buf.WriteString(";")
	return p.db.execStmt(&stmt{
		statement: buf,
	})
}
