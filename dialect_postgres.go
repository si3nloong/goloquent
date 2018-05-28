package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
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
	conf.trimSpace()
	addr, buf := "@", new(bytes.Buffer)
	buf.WriteString("postgres://")
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
	buf.WriteString("?sslmode=disable")
	fmt.Println("Connection String :: ", buf.String())
	client, err := sql.Open("postgres", buf.String())
	if err != nil {
		return nil, err
	}
	return client, nil
}

// GetTable :
func (p *postgres) GetTable(name string) string {
	return fmt.Sprintf("%q", name)
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
func (p *postgres) CreateIndex(idx string, cols []string) string {
	return fmt.Sprintf("CREATE INDEX %s (%s)",
		p.Quote(idx),
		p.Quote(strings.Join(cols, ",")))
}

func (p *postgres) Quote(n string) string {
	return fmt.Sprintf("%q", n)
}

func (p *postgres) Bind(i int) string {
	return fmt.Sprintf("$%d", i)
}

// DataType :
func (p *postgres) DataType(sc Schema) string {
	buf := new(bytes.Buffer)
	buf.WriteString(sc.DataType)
	if sc.IsUnsigned {
		buf.WriteString(fmt.Sprintf(" CHECK (%s > 0)", p.Quote(sc.Name)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		t := reflect.TypeOf(sc.DefaultValue)
		if t != reflect.TypeOf(OmitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", p.toString(sc.DefaultValue)))
		}
	}
	// if sc.CharSet != nil {
	// 	buf.WriteString(fmt.Sprintf(" CHARACTER SET %s COLLATE %s",
	// 		p.Quote(sc.CharSet.Encoding),
	// 		p.Quote(sc.CharSet.Collation)))
	// }

	return buf.String()
}

func (p *postgres) OnConflictUpdate(cols []string) string {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf(
		"ON CONFLICT (%s,%s) UPDATE SET",
		p.Quote(parentColumn),
		p.Quote(keyColumn)))
	for _, c := range cols {
		buf.WriteString(fmt.Sprintf("%s = %s", p.Quote(c), p.Quote(c)))
	}
	return buf.String()
}

func (p *postgres) GetSchema(c Column) []Schema {
	f := c.field
	t := f.getRoot().typeOf
	if f.isFlatten() {
		t = f.typeOf
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
					Schema{keyColumn, fmt.Sprintf("varchar(%d)", 50), OmitDefault(nil), false, false, false, latin2CharSet},
					Schema{parentColumn, fmt.Sprintf("varchar(%d)", 512), OmitDefault(nil), false, false, false, latin2CharSet},
				}
			}
			sc.IsIndexed = true
			sc.DataType = fmt.Sprintf("varchar(%d)", 512)
			sc.CharSet = latin2CharSet
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
		sc.DataType = "timestamptz"
	default:
		switch t.Kind() {
		case reflect.String:
			sc.DefaultValue = ""
			sc.DataType = fmt.Sprintf("varchar(%d)", 191)
			if f.isLongText() {
				sc.DefaultValue = nil
				sc.DataType = "text"
			}
			sc.CharSet = utf8CharSet
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
			sc.DataType = "text"
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

func (p *postgres) toString(it interface{}) string {
	var v string
	switch vi := it.(type) {
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
	case nil:
		v = "NULL"
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}
