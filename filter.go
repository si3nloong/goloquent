package goloquent

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

// Filter :
type Filter struct {
	field    string
	operator operator
	value    interface{}
	isJSON   bool
}

// Field :
func (f Filter) Field() string {
	return f.field
}

// IsJSON :
func (f Filter) IsJSON() bool {
	return f.isJSON
}

// JSON :
type JSON struct {
}

// JSON :
func (f Filter) JSON() *JSON {
	return &JSON{}
}

// Interface :
func (f *Filter) Interface() (interface{}, error) {
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
	if val == nil {
		return nil, nil
	}
	v := reflect.ValueOf(val)
	var it interface{}
	t := v.Type()
	switch vi := v.Interface().(type) {
	case *datastore.Key:
		if vi == nil {
			return nil, nil
		}
		it = vi
	case datastore.GeoPoint:
		it = geoLocation{vi.Lat, vi.Lng}
	case time.Time:
		it = vi
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
