package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

type postgres struct {
	sequel
}

var _ Dialect = new(postgres)

func init() {
	RegisterDialect("postges", new(postgres))
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
	buf.WriteString("?sslmode=verify-full")
	fmt.Println("Connection String :: ", buf.String())
	client, err := sql.Open("postgres", buf.String())
	if err != nil {
		return nil, err
	}
	return client, nil
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
		buf.WriteString(fmt.Sprintf(" CHECK (%s > 0)", sc.Name))
	}
	if sc.CharSet != nil {
		buf.WriteString(fmt.Sprintf(" CHARACTER SET %s COLLATE %s",
			p.Quote(sc.CharSet.Encoding),
			p.Quote(sc.CharSet.Collation)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		t := reflect.TypeOf(sc.DefaultValue)
		if t != reflect.TypeOf(OmitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", p.toString(sc.DefaultValue)))
		}
	}
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
			sc.CharSet = utf8mb4CharSet
		case reflect.Int8:
		case reflect.Int16:
		case reflect.Int, reflect.Int32:
		case reflect.Int64:
		case reflect.Uint8:
		case reflect.Uint16:
		case reflect.Uint, reflect.Uint32:
		case reflect.Uint64:
		case reflect.Float32, reflect.Float64:
		default:
		}
	}

	return []Schema{sc}
}
