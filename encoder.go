package goloquent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

func flatMap(data map[string]interface{}) map[string]interface{} {
	for k, value := range data {
		switch vi := value.(type) {
		case map[string]interface{}:
			fm := flatMap(vi)
			for kk, nv := range fm {
				data[k+"."+kk] = nv
			}
			delete(data, k)
		default:
			data[k] = value
		}
	}

	return data
}

func normalize(f field, it interface{}) ([]Property, error) {
	props := make([]Property, 0)
	if f.isFlatten() {
		if f.isSlice() {
			vals := make(map[string][]interface{})
			for _, vi := range it.([]interface{}) {
				l := flatMap(vi.(map[string]interface{}))
				for k, vv := range l {
					name := f.name + "." + k
					arr := vals[name]
					arr = append(arr, vv)
					vals[name] = arr
				}
			}

			for k, vv := range vals {
				props = append(props, Property{[]string{k}, f.typeOf, vv})
			}
			return props, nil
		}

		for k, vv := range flatMap(it.(map[string]interface{})) {
			props = append(props, Property{[]string{f.name, k}, f.typeOf, vv})
		}
		return props, nil
	}

	props = append(props, Property{[]string{f.name}, f.typeOf, it})
	return props, nil
}

func marshal(it interface{}) (interface{}, error) {
	switch v := it.(type) {
	case []interface{}, map[string]interface{}, json.RawMessage:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("goloquent: unable to marshal the value %v", v)
		}
		return string(b), nil
	}
	return it, nil
}

// SaveStruct :
func SaveStruct(src interface{}) (map[string]Property, error) {
	vi := reflect.Indirect(reflect.ValueOf(src))
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi) // copy the value to new struct

	ety, err := newEntity(vv.Interface())
	if err != nil {
		return nil, err
	}

	data := make(map[string]Property)
	for _, f := range ety.codec.fields {
		fv := getFieldByIndex(vv.Elem(), f.paths)
		var it, err = saveField(f, fv)
		if err != nil {
			return nil, err
		}
		props, err := normalize(f, it)
		if err != nil {
			return nil, err
		}
		for _, p := range props {
			data[p.Name()] = p
		}
	}

	vi.Set(vv.Elem())
	return data, nil
}

func mapToValue(data map[string]interface{}) (map[string]interface{}, error) {
	for k, val := range data {
		var (
			it  interface{}
			err error
		)
		switch vi := val.(type) {
		case map[string]interface{}:
			it, err = mapToValue(vi)
			if err != nil {
				return nil, err
			}
		default:
			it, err = interfaceToValue(val)
			if err != nil {
				return nil, err
			}
		}

		data[k] = it
	}
	return data, nil
}

// LoadStruct :
func LoadStruct(src interface{}, data map[string]interface{}) error {
	v := reflect.ValueOf(src)
	if v.Type().Kind() != reflect.Ptr {
		return fmt.Errorf("goloquent: struct is not addressable")
	}
	codec, err := getStructCodec(src)
	if err != nil {
		return err
	}

	nv := reflect.New(v.Type().Elem())
	for _, f := range codec.fields {
		fv := getField(nv.Elem(), f.paths)
		if err := loadField(fv, data[f.name]); err != nil {
			return err
		}
	}

	v.Elem().Set(nv.Elem())
	return nil
}

func interfaceToValue(it interface{}) (interface{}, error) {
	var value interface{}

	switch vi := it.(type) {
	case nil:
		value = vi
	case string:
		value = vi
	case bool:
		value = vi
	case int, int8, int16, int32, int64:
		value = vi
	case uint, uint8, uint16, uint32, uint64:
		value = vi
	case float32, float64:
		value = vi
	case json.RawMessage:
		value = vi
	case []byte:
		value = base64.StdEncoding.EncodeToString(vi)
	case *datastore.Key:
		str := stringifyKey(vi)
		if str == "" {
			value = nil
		} else {
			value = str
		}
	case SoftDelete:
		vv := reflect.ValueOf(it)
		if vv.IsNil() {
			return nil, nil
		}
		value = (*SoftDelete(vi)).UTC().Format("2006-01-02 15:04:05")
	case Date:
		value = time.Time(vi).Format("2006-01-02")
	case time.Time:
		value = vi.UTC().Format("2006-01-02 15:04:05")
	case geoLocation:
		b, _ := json.Marshal(vi)
		value = json.RawMessage(b)
	case []interface{}:
		slice := make([]interface{}, 0, len(vi))
		for _, elem := range vi {
			s, err := interfaceToValue(elem)
			if err != nil {
				return nil, err
			}
			slice = append(slice, s)
		}
		value = slice
	case map[string]interface{}: // Nested struct
		var list, err = mapToValue(vi)
		if err != nil {
			return nil, err
		}
		value = list
	default:
		vv := reflect.ValueOf(it)
		if vv.Type().Kind() != reflect.Ptr {
			return nil, fmt.Errorf("goloquent: invalid data type %v", vv.Type())
		}
		if vv.IsNil() {
			return nil, nil
		}
		it, err := interfaceToValue(vv.Elem().Interface())
		if err != nil {
			return nil, err
		}
		return it, nil
	}

	return value, nil
}

func saveSliceField(f field, v reflect.Value) ([]interface{}, error) {
	// if it's struct, f.StructCodec is not nil
	if v.Len() <= 0 {
		return make([]interface{}, 0), nil
	}

	slice := make([]interface{}, 0)
	for i := 0; i < v.Len(); i++ {
		var it, err = saveField(f, v.Index(i))
		if err != nil {
			return nil, err
		}
		slice = append(slice, it)
	}

	return slice, nil
}

func saveStructField(sc *StructCodec, v reflect.Value) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	for _, f := range sc.fields {
		fv := getFieldByIndex(v, f.paths)
		if !fv.IsValid() {
			continue
		}

		var it, err = saveField(f, fv)
		if err != nil {
			return nil, err
		}

		data[f.name] = it
	}

	return data, nil
}

func saveField(f field, v reflect.Value) (interface{}, error) {
	var it interface{}
	t := v.Type()

	switch vi := v.Interface().(type) {
	case *datastore.Key, time.Time:
		it = vi
	case json.RawMessage:
		if vi == nil {
			return json.RawMessage("null"), nil
		}
		it = vi
	case Date:
		it = vi
	case datastore.GeoPoint:
		it = geoLocation{vi.Lat, vi.Lng}
	case SoftDelete:
		if v.IsNil() {
			return reflect.Zero(typeOfSoftDelete).Interface(), nil
		}
		it = vi
	default:
		switch t.Kind() {
		case reflect.String:
			it = v.String()
		case reflect.Bool:
			it = v.Bool()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			it = v.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			it = v.Uint()
		case reflect.Float32, reflect.Float64:
			it = v.Float()
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				it = v.Bytes()
			} else {
				v = initSlice(v) // initialize the slice if it's nil
				return saveSliceField(f, v)
			}
		case reflect.Ptr:
			elem := t.Elem()
			if isBaseType(elem) {
				if v.IsNil() { // return zero which has datatype
					return reflect.Zero(t).Interface(), nil
				}
				return saveField(f, v.Elem())
			}
			if elem.Kind() != reflect.Struct {
				return nil, fmt.Errorf("goloquent: unsupported struct field data type %q", t.String())
			}
			if v.IsNil() {
				return reflect.Zero(t).Interface(), nil
			}
			v = v.Elem()
			fallthrough
		case reflect.Struct:
			data, err := saveStructField(f.StructCodec, v)
			if err != nil {
				return nil, err
			}
			it = data

		default:
			return nil, fmt.Errorf("goloquent: unsupported struct field data type %q", t.String())
		}
	}

	return it, nil
}
