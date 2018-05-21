package goloquent

import (
	"fmt"
)

type postgres struct {
	sequel
}

func init() {
	RegisterDialect("postges", new(postgres))
}

func (p *postgres) Bind(i int) string {
	return fmt.Sprintf("$%d", i)
}

func (p *postgres) Quote(n string) string {
	return fmt.Sprintf("%q", n)
}

func (p *postgres) OnConflictUpdate(cols []string) string {
	return ""
}

// func (p *postgres) GetSchema(f field) Schema {
// 	t := f.getRoot().typeOf
// 	sc := Schema{}
// 	if t.Kind() == reflect.Ptr {
// 		sc.IsNullable = true
// 		if t == typeOfPtrKey {
// 			sc.isIndexed = true
// 			sc.dataType = fmt.Sprintf("varchar(%d)", 512)
// 			return sc
// 		}
// 		t = t.Elem()
// 	}

// 	switch t {
// 	case typeOfByte:
// 		sc.dataType = "bytea"
// 	case typeOfTime:
// 		sc.defaultValue = time.Time{}
// 		sc.dataType = "timestamptz"
// 	default:
// 		switch t.Kind() {
// 		case reflect.String:
// 			sc.defaultValue = ""
// 			sc.dataType = fmt.Sprintf("varchar(%d)", 255)
// 		case reflect.Bool:
// 			sc.defaultValue = false
// 			sc.dataType = "boolean"
// 		case reflect.Int8:
// 			sc.defaultValue = int8(0)
// 			sc.dataType = "smallint"
// 		case reflect.Int, reflect.Int16, reflect.Int32:
// 			sc.defaultValue = int(0)
// 			sc.dataType = "integer"
// 		case reflect.Int64:
// 			sc.defaultValue = int64(0)
// 			sc.dataType = "bigint"
// 		case reflect.Float32, reflect.Float64:
// 			sc.defaultValue = float64(0)
// 			sc.dataType = "real"
// 		default:
// 			sc.dataType = "text"
// 		}
// 	}

// 	return sc
// }
