package goloquent

import (
	"fmt"
	"reflect"
	"strings"
)

type column struct {
	names []string
	field field
}

func (c column) Name() string {
	return strings.Join(c.names, ".")
}

func getColumns(prefix []string, codec *StructCodec) []column {
	columns := make([]column, 0)
	for _, f := range codec.fields {
		c := make([]column, 0)
		if f.getRoot().isFlatten() && f.StructCodec != nil {
			c = getColumns(append(prefix, f.name), f.StructCodec)
		} else {
			c = append(c, column{
				names: append(prefix, f.name),
				field: f,
			})
		}
		columns = append(columns, c...)
	}

	return columns
}

type entity struct {
	name       string
	typeOf     reflect.Type
	isMultiPtr bool
	slice      reflect.Value
	codec      *StructCodec
	fields     map[string]column
	cols       []column
}

// convertMulti will convert any single model to pointer of []model
func convertMulti(v reflect.Value) reflect.Value {
	vi := reflect.MakeSlice(reflect.SliceOf(v.Type()), 1, 1)
	vi.Index(0).Set(v)
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	return vv
}

func newEntity(it interface{}) (*entity, error) {
	v := reflect.ValueOf(it)
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("goloquent: model is not addressable")
	}

	isMultiPtr := false
	t := v.Type().Elem()
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			isMultiPtr = true
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return nil, fmt.Errorf("goloquent: invalid entity data type : %v, it should be struct", t)
		}
	case reflect.Struct:
		isMultiPtr = true
		v = convertMulti(v)
	default:
		return nil, fmt.Errorf("goloquent: invalid entity data type : %v, it should be struct", t)
	}

	codec, err := getStructCodec(reflect.New(t).Interface())
	if err != nil {
		return nil, err
	}

	fields := make(map[string]column)
	cols := getColumns(nil, codec)
	for _, c := range cols {
		fields[c.Name()] = c
	}

	return &entity{
		name:       t.Name(),
		typeOf:     t,
		isMultiPtr: isMultiPtr,
		codec:      codec,
		slice:      v,
		fields:     fields,
		cols:       cols,
	}, nil
}

func (e *entity) field(key string) field {
	return e.fields[key].field
}

func (e *entity) Name() string {
	return e.name
}

func (e *entity) Columns() (cols []string) {
	cols = make([]string, 0, len(e.cols))
	for _, c := range e.cols {
		// fmt.Println(c.field.getFullPath())
		if c.Name() == keyFieldName {
			cols = append(cols, keyColumn, parentColumn)
			continue
		}
		cols = append(cols, c.Name())
	}
	return
}
