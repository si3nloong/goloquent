package goloquent

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

type columner interface {
	Name() string
	IsJSON() bool
}

type jsonColumn struct {
	name string
}

func (c jsonColumn) Name() string {
	return c.name
}

func (c jsonColumn) IsJSON() bool {
	return true
}

type rawColumn struct {
	name string
}

func (c rawColumn) Name() string {
	return c.name
}

func (c rawColumn) IsJSON() bool {
	return false
}

// Filter :
type Filter struct {
	columner
	operator operator
	value    interface{}
}

// Interface :
func (f *Filter) Interface() (interface{}, error) {
	if f.value == nil {
		return nil, nil
	}
	v, err := normalizeValue(f.value)
	if err != nil {
		return nil, err
	}
	return interfaceToValue(v)
}

// final data type :
// string, bool, uint64, int64, float64, []byte
// time.Time, *datastore.Key, datastore.GeoPoint, []interface{}
func normalizeValue(val interface{}) (interface{}, error) {
	v := reflect.ValueOf(val)
	var it interface{}
	t := v.Type()
	switch vi := v.Interface().(type) {
	case *datastore.Key:
		if vi == nil {
			return nil, nil
		}
		it = vi
	case time.Time:
		it = vi
	case datastore.GeoPoint:
		it = geoLocation{vi.Lat, vi.Lng}
	default:
		switch t.Kind() {
		case reflect.String:
			it = v.String()
		case reflect.Bool:
			it = v.Bool()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			it = v.Uint()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			it = v.Int()
		case reflect.Float32, reflect.Float64:
			it = v.Float()
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				return v.Bytes(), nil
			}
			arr := make([]interface{}, 0, v.Len())
			for i := 0; i < v.Len(); i++ {
				vv := v.Index(i)
				var vi, err = normalizeValue(vv.Interface())
				if err != nil {
					return vi, err
				}
				arr = append(arr, vi)
			}
			it = arr
		case reflect.Ptr:
			if v.IsNil() {
				return nil, nil
			}
			var val, err = normalizeValue(v.Elem().Interface())
			if err != nil {
				return nil, err
			}
			vv := reflect.New(v.Type().Elem())
			vv.Elem().Set(reflect.ValueOf(val))
			it = vv.Interface()
		default:
			return nil, fmt.Errorf("goloquent: unsupported data type %v", t)
		}
	}

	return it, nil
}
