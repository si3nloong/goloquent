package goloquent

import (
	"fmt"
	"reflect"
	"strings"
)

// Column :
type Column struct {
	names []string
	field field
}

// Name :
func (c Column) Name() string {
	return strings.Join(c.names, ".")
}

func getColumns(prefix []string, codec *StructCodec) []Column {
	columns := make([]Column, 0)
	for _, f := range codec.fields {
		c := make([]Column, 0)
		if f.getRoot().isFlatten() && f.StructCodec != nil {
			c = getColumns(append(prefix, f.name), f.StructCodec)
		} else {
			c = append(c, Column{
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
	fields     map[string]Column
	columns    []Column
}

// convertMulti will convert any single model to pointer of []model
func convertMulti(v reflect.Value) reflect.Value {
	vi := reflect.MakeSlice(reflect.SliceOf(v.Type()), 1, 1)
	vi.Index(0).Set(v)
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	return vv
}

// TODO: check primary key must present
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

	fields := make(map[string]Column)
	cols := getColumns(nil, codec)
	for _, c := range cols {
		fields[c.Name()] = c
	}

	if _, hasKey := fields[keyFieldName]; !hasKey {
		return nil, fmt.Errorf("goloquent: entity %v doesn't has primary key property", t)
	}

	return &entity{
		name:       t.Name(),
		typeOf:     t,
		isMultiPtr: isMultiPtr,
		codec:      codec,
		slice:      v,
		fields:     fields,
		columns:    cols,
	}, nil
}

func (e *entity) setName(name string) {
	name = strings.TrimSpace(name)
	if name != "" {
		e.name = name
	}
}

func (e *entity) field(key string) field {
	return e.fields[key].field
}

func (e *entity) Name() string {
	return e.name
}

func (e *entity) Columns() (cols []string) {
	cols = make([]string, 0, len(e.columns))
	for _, c := range e.columns {
		if c.Name() == keyFieldName {
			cols = append(cols, keyColumn, parentColumn)
			continue
		}
		cols = append(cols, c.Name())
	}
	return
}
