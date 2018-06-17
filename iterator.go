package goloquent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

// Loader :
type Loader interface {
	Load() error
}

// Saver :
type Saver interface {
	Save() error
}

// Iterator :
type Iterator struct {
	table    string
	position int
	columns  []string
	results  []map[string][]byte
	cursor   *datastore.Key
}

func (it *Iterator) mergeKey() {
	pos := len(it.results) - 1
	l := it.results[pos]
	if _, isOk := l[parentColumn]; !isOk {
		return
	}
	if _, isOk := l[keyColumn]; !isOk {
		return
	}
	buf := new(bytes.Buffer)
	buf.Write(l[parentColumn])
	buf.WriteString(keyDelimeter)
	buf.WriteString(it.table + ",")
	buf.Write(l[keyColumn])
	l[keyFieldName] = buf.Bytes()
	it.results[pos] = l
}

func (it *Iterator) patchKey() {
	pos := len(it.results) - 1
	l := it.results[pos]
	if _, isOk := l[pkColumn]; !isOk {
		return
	}
	paths := bytes.Split(l[pkColumn], []byte(`/`))
	last := len(paths) - 1
	kk := paths[last]
	paths = paths[:last]
	buf := new(bytes.Buffer)
	buf.Write(bytes.Join(paths, []byte(keyDelimeter)))
	buf.WriteString(keyDelimeter)
	buf.WriteString(it.table + ",")
	buf.Write(kk)
	l[keyFieldName] = buf.Bytes()
	it.results[pos] = l
}

func (it *Iterator) put(pos int, k string, v interface{}) error {
	diff := pos - len(it.results) + 1
	for i := 0; i < diff; i++ {
		it.results = append(it.results, make(map[string][]byte))
	}
	l := it.results[pos]

	var b []byte
	switch vi := v.(type) {
	case nil:
	case time.Time:
		b = []byte(vi.Format("2006-01-02 15:04:05"))
	case []byte:
		b = vi
	default:
		b = []byte(fmt.Sprintf("%v", vi))
	}
	l[k] = b
	it.results[pos] = l
	return nil
}

// First :
func (it *Iterator) First() *Iterator {
	it.position = 0
	if len(it.results) <= 0 {
		return nil
	}
	return it
}

// Last :
func (it *Iterator) Last() *Iterator {
	i := len(it.results) - 1
	if i < 0 {
		return nil
	}
	it.position = i
	return it
}

// Get : get value by key
func (it *Iterator) Get(k string) []byte {
	l := it.results[it.position]
	return l[k]
}

// Count : return the records count
func (it *Iterator) Count() uint {
	return uint(len(it.results))
}

// // Cursor :
// func (it *Iterator) Cursor() (*datastore.Key, error) {
// 	return &datastore.Key{}, nil
// }

// Next : go next record
func (it *Iterator) Next() bool {
	it.position++
	if it.position > len(it.results)-1 {
		return false
	}
	return true
}

func (it *Iterator) scan(src interface{}) (map[string]interface{}, error) {
	v := reflect.ValueOf(src)
	if v.Type().Kind() != reflect.Ptr {
		return nil, fmt.Errorf("goloquent: struct is not addressable")
	}
	codec, err := getStructCodec(src)
	if err != nil {
		return nil, err
	}

	nv := reflect.New(v.Type().Elem())
	data := make(map[string]interface{})
	for _, f := range codec.fields {
		fv := getField(nv.Elem(), f.paths)
		props := getTypes(nil, f, f.isFlatten())
		for i, p := range props {
			k := p.Name()
			b := it.Get(k)
			var vv, err = valueToInterface(p.typeOf, b)
			if err != nil {
				return nil, err
			}
			props[i].Value = vv
		}

		vi := denormalize(f, props)
		data[f.name] = vi
		if err := loadField(fv, vi); err != nil {
			return nil, err
		}
	}

	if l, isOk := nv.Interface().(Loader); isOk {
		if err := l.Load(); err != nil {
			return nil, fmt.Errorf("goloquent: %v", err)
		}
	}

	v.Elem().Set(nv.Elem())
	return data, nil
}

// Scan : set the model value
func (it *Iterator) Scan(src interface{}) error {
	if _, err := it.scan(src); err != nil {
		return err
	}
	return nil
}

func getField(v reflect.Value, path []int) reflect.Value {
	for i, p := range path {
		v = v.Field(p)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			if i < len(path)-1 {
				v = v.Elem()
			}
		}
	}
	return v
}

// Property :
type Property struct {
	name   []string
	typeOf reflect.Type
	Value  interface{}
}

func (p Property) isZero() bool {
	return interfaceIsZero(p.Value)
}

// Name :
func (p Property) Name() string {
	return strings.Join(p.name, ".")
}

// Interface :
func (p Property) Interface() (interface{}, error) {
	vv, err := interfaceToValue(p.Value)
	if err != nil {
		return nil, err
	}
	vv, err = marshal(vv)
	if err != nil {
		return nil, err
	}
	return vv, err
}

func getTypes(ns []string, f field, isFlatten bool) []Property {
	props := make([]Property, 0)
	if isFlatten && f.StructCodec != nil {
		codec := f.StructCodec
		for _, sf := range codec.fields {
			dd := getTypes(append(ns, f.name), sf, isFlatten)
			props = append(props, dd...)
		}
		return props
	}

	t := f.typeOf
	root := f.getRoot()
	if isFlatten {
		if root.isSlice() && root.typeOf != f.typeOf {
			t = reflect.MakeSlice(reflect.SliceOf(t), 0, 0).Type()
		}
	}

	d := Property{append(ns, f.name), t, nil}
	props = append(props, d)
	return props
}

func interfaceIsZero(it interface{}) bool {
	var zero bool
	switch vi := it.(type) {
	case string:
		zero = len(vi) == 0
	case bool:
		zero = vi == false
	case []byte:
		zero = len(vi) == 0
	case json.RawMessage:
		zero = len(vi) == 0
	case int64:
		zero = vi == 0
	case uint64:
		zero = vi == 0
	case float64:
		zero = vi == 0
	case SoftDelete:
		zero = vi == SoftDelete(nil)
	case *datastore.Key:
		zero = vi == nil || (*vi) == datastore.Key{}
	case datastore.GeoPoint:
		zero = vi == datastore.GeoPoint{}
	case time.Time:
		zero = vi == time.Time{}
	case []interface{}:
		zero = len(vi) == 0
	case map[string]interface{}:
		if len(vi) == 0 {
			return true
		}
		allZero := true
		for _, v := range vi {
			if isZero := interfaceIsZero(v); !isZero {
				return false
			}
		}
		zero = allZero
	default:
		vv := reflect.ValueOf(vi)
		if vv.Type().Kind() == reflect.Ptr && vv.IsNil() {
			return true
		}

		return reflect.DeepEqual(it, reflect.Zero(vv.Type()).Interface())
	}
	return zero
}
