package goloquent

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

// SoftDelete :
type SoftDelete *time.Time

type geoLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func unmarshalStruct(t reflect.Type, l map[string]*json.RawMessage) (map[string]interface{}, error) {
	codec, err := getStructCodec(reflect.New(t).Interface())
	if err != nil {
		return nil, err
	}

	data := make(map[string]interface{})
	for _, f := range codec.fields {
		b, isOk := l[f.name]
		if !isOk {
			continue
		}
		var it, err = valueToInterface(f.typeOf, getByte(b))
		if err != nil {
			return nil, err
		}
		data[f.name] = it
	}

	return data, nil
}

func getByte(v *json.RawMessage) []byte {
	var b []byte
	if v == nil {
		return b
	}
	return []byte(*v)
}

func escape(b []byte) string {
	return strings.Trim(strings.TrimSpace(string(b)), `"`)
}

// covert byte to standard data type
// string, bool, int64, float64, []byte
// *datastore.Key, time.Time, datastore.GeoPoint
// []interface{}, *struct
func valueToInterface(t reflect.Type, v []byte) (interface{}, error) {
	var it interface{}

	switch t {
	case typeOfPtrKey:
		if v == nil {
			var key *datastore.Key
			return key, nil
		}
		var k, err = parseKey(string(v))
		if err != nil {
			return nil, err
		}
		it = k
	case typeOfJSONRawMessage:
		if v == nil || fmt.Sprintf("%v", v) == "null" {
			return json.RawMessage(nil), nil
		}
		it = json.RawMessage(v)
	case typeOfTime:
		if v == nil {
			return time.Time{}, nil
		}
		var dt, err = time.Parse("2006-01-02 15:04:05", escape(v))
		if err != nil {
			return nil, fmt.Errorf("goloquent: unable to parse %q to date time", string(v))
		}
		it = dt
	case typeOfDate:
		if v == nil {
			return Date(time.Time{}), nil
		}
		var dt, err = time.Parse("2006-01-02 15:04:05", escape(v))
		if err != nil {
			return nil, fmt.Errorf("goloquent: unable to parse %q to date", string(v))
		}
		it = Date(dt)
	case typeOfSoftDelete:
		if v == nil {
			return SoftDelete(nil), nil
		}
		var dt, err = time.Parse("2006-01-02 15:04:05", escape(v))
		if err != nil {
			return nil, fmt.Errorf("goloquent: unable to parse %q to soft delete date time", string(v))
		}
		it = SoftDelete(&dt)
	case typeOfByte:
		if v == nil {
			var b []byte
			return b, nil
		}
		var b, err = base64.StdEncoding.DecodeString(escape(v))
		if err != nil {
			return nil, fmt.Errorf("goloquent: corrupted bytes, %q", string(v))
		}
		it = b
	case typeOfGeoPoint:
		if v == nil || fmt.Sprintf("%v", v) == "null" {
			return datastore.GeoPoint{}, nil
		}
		var g geoLocation
		if err := json.Unmarshal(bytes.Trim(v, `"`), &g); err != nil {
			return nil, fmt.Errorf("goloquent: corrupted geolocation value, %s", string(v))
		}
		it = datastore.GeoPoint{Lat: g.Latitude, Lng: g.Longitude}
	default:
		switch t.Kind() {
		case reflect.String:
			if v == nil {
				return "", nil
			}
			it = escape(v)
		case reflect.Bool:
			if v == nil {
				return false, nil
			}
			var b, err = strconv.ParseBool(escape(v))
			if err != nil {
				return nil, fmt.Errorf("goloquent: unable to parse %q to boolean", string(v))
			}
			it = b
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if v == nil {
				return int64(0), nil
			}
			var n, err = strconv.ParseFloat(escape(v), 64)
			if err != nil {
				return nil, fmt.Errorf("goloquent: unable to parse %q to int64", string(v))
			}
			it = int64(n)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if v == nil {
				return uint64(0), nil
			}
			var n, err = strconv.ParseFloat(escape(v), 64)
			if err != nil {
				return nil, fmt.Errorf("goloquent: unable to parse %q to uint64", string(v))
			}
			it = uint64(n)
		case reflect.Float32, reflect.Float64:
			if v == nil {
				return float64(0), nil
			}
			var f, err = strconv.ParseFloat(escape(v), 64)
			if err != nil {
				return nil, err
			}
			it = f
		case reflect.Slice, reflect.Array:
			if v == nil || fmt.Sprintf("%v", v) == "null" {
				var arr []interface{}
				return arr, nil
			}
			var b []*json.RawMessage
			if err := json.Unmarshal(v, &b); err != nil {
				return nil, fmt.Errorf("goloquent: corrupted slice value, %v", err)
			}

			arr := make([]interface{}, 0, len(b))
			for i := 0; i < len(b); i++ {
				var vv, err = valueToInterface(t.Elem(), getByte(b[i]))
				if err != nil {
					return nil, err
				}
				arr = append(arr, vv)
			}
			it = arr
		case reflect.Ptr:
			if isBaseType(t.Elem()) {
				if v == nil {
					return reflect.Zero(t).Interface(), nil
				}
				var it, err = valueToInterface(t.Elem(), v)
				if err != nil {
					return nil, err
				}
				return &it, nil
			}
			if t.Elem().Kind() != reflect.Struct {
				return nil, fmt.Errorf("goloquent: unsupported struct field data type %q", t.String())
			}

			if v == nil || fmt.Sprintf("%v", v) == "null" {
				return reflect.Zero(t).Interface(), nil
			}
			t = t.Elem()
			fallthrough
		case reflect.Struct:
			if v == nil || fmt.Sprintf("%v", v) == "null" {
				var l map[string]interface{}
				return l, nil
			}
			var l = make(map[string]*json.RawMessage)
			if err := json.Unmarshal(v, &l); err != nil {
				return nil, fmt.Errorf("goloquent: unmatched struct layout with value")
			}
			if len(l) <= 0 {
				return make(map[string]interface{}), nil
			}

			var err error
			it, err = unmarshalStruct(t, l)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("goloquent: unmatched data type %q", t.String())
		}
	}

	return it, nil
}

func loadStructField(v reflect.Value, l map[string]interface{}) error {
	var codec, err = getStructCodec(v.Interface())
	if err != nil {
		return err
	}

	for _, f := range codec.fields {
		val, isOk := l[f.name]
		vi := getField(v, f.paths)
		if !isOk {
			// vi.Set(reflect.Zero(vi.Type()))
			continue
		}

		if err := loadField(vi, val); err != nil {
			return err
		}
	}

	return nil
}

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < v.Len(); i++ {
			z = z && isZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		for i := 0; i < v.NumField(); i++ {
			z = z && isZero(v.Field(i))
		}
		return z
	}
	z := reflect.Zero(v.Type())
	return v.Interface() == z.Interface()
}

func loadField(v reflect.Value, it interface{}) error {
	switch v.Kind() {
	case reflect.String:
		x, isOk := it.(string)
		if !isOk {
			return unmatchDataType(x, it)
		}
		v.SetString(x)
	case reflect.Bool:
		x, isOk := it.(bool)
		if !isOk {
			return unmatchDataType(x, it)
		}
		v.SetBool(x)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		x, isOk := it.(int64)
		if !isOk {
			return unmatchDataType(x, it)
		}
		if v.OverflowInt(x) {
			return fmt.Errorf("goloquent: overflow %s value %v", v.Kind(), it)
		}
		v.SetInt(x)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x, isOk := it.(uint64)
		if !isOk {
			return unmatchDataType(x, it)
		}
		if v.OverflowUint(x) {
			return fmt.Errorf("goloquent: overflow %s value %v", v.Kind(), it)
		}
		v.SetUint(x)
	case reflect.Float32, reflect.Float64:
		x, isOk := it.(float64)
		if !isOk {
			return unmatchDataType(x, it)
		}
		if v.OverflowFloat(x) {
			return fmt.Errorf("goloquent: overflow %s value %v", v.Kind(), it)
		}
		v.SetFloat(x)
	case reflect.Ptr:
		vi := reflect.ValueOf(it)
		if vi.IsNil() {
			v.Set(reflect.New(v.Type()).Elem())
			return nil
		}

		elem := v.Type().Elem()
		switch {
		case v.Type() == typeOfPtrKey:
			x, isOk := it.(*datastore.Key)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.Set(reflect.ValueOf(x))
		case isBaseType(elem):
			if err := loadField(v.Elem(), reflect.ValueOf(it).Elem().Interface()); err != nil {
				return err
			}
		case elem.Kind() == reflect.Struct:
			if vi.IsNil() {
				v.Set(reflect.Zero(v.Type()))
				return nil
			}

			v = initStruct(v)
			x, isOk := it.(map[string]interface{})
			if !isOk {
				return unmatchDataType(x, it)
			}

			if err := loadStructField(v.Elem(), x); err != nil {
				return err
			}
		default:
			return unmatchDataType(v, it)
		}

	case reflect.Struct:
		switch v.Type() {
		case typeOfGeoPoint:
			x, isOk := it.(datastore.GeoPoint)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.Set(reflect.ValueOf(x))
		case typeOfTime:
			x, isOk := it.(time.Time)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.Set(reflect.ValueOf(x))
		case typeOfDate:
			x, isOk := it.(Date)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.Set(reflect.ValueOf(x))
		case typeOfSoftDelete:
			x, isOk := it.(SoftDelete)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.Set(reflect.ValueOf(x))
		default:
			x, isOk := it.(map[string]interface{})
			if !isOk {
				return unmatchDataType(x, it)
			}

			v = initStruct(v)
			if err := loadStructField(v, x); err != nil {
				return err
			}
		}

	case reflect.Slice, reflect.Array:
		switch v.Type() {
		case typeOfByte:
			x, isOk := it.([]byte)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.SetBytes(x)
		case typeOfJSONRawMessage:
			x, isOk := it.(json.RawMessage)
			if !isOk {
				return unmatchDataType(x, it)
			}
			v.Set(reflect.ValueOf(x))

		default:
			x, isOk := it.([]interface{})
			if !isOk {
				return unmatchDataType(x, it)
			}

			arr := reflect.MakeSlice(v.Type(), len(x), len(x))
			for i, xv := range x {
				if err := loadField(arr.Index(i), xv); err != nil {
					return err
				}
			}
			v.Set(arr)

		}

	default:
		return fmt.Errorf("goloquent: unsupported data type, %v", v.Type())
	}

	return nil
}

func initStruct(v reflect.Value) reflect.Value {
	t := v.Type()
	if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
		if v.IsNil() {
			v.Set(reflect.New(t.Elem()))
		}
	}
	return v
}

func unmatchDataType(o interface{}, p interface{}) error {
	return fmt.Errorf("goloquent: unmatched data type of original, %v versus %v", reflect.TypeOf(o), reflect.TypeOf(p))
}

func unflatMap(l map[string]interface{}, names []string, it interface{}) {
	for i, k := range names {
		if i == len(names)-1 {
			l[k] = it
			continue
		}
		_, isExist := l[k]
		if !isExist {
			l[k] = make(map[string]interface{})
		}
		l = (l[k]).(map[string]interface{})
	}
}

// Denormalize flatten field
// from []interface{} to []map[string]interface{}
// or from interface{} to map[string]interface{}
func denormalize(f field, values []Property) interface{} {
	if f.isFlatten() {
		if f.isSlice() {
			arr := make([]interface{}, 0)
			for _, vv := range values {
				for i, vi := range vv.Value.([]interface{}) {
					if i > len(arr)-1 {
						arr = append(arr, make(map[string]interface{}))
					}
					l := arr[i].(map[string]interface{})
					unflatMap(l, vv.name[1:], vi)
					arr[i] = l
				}
			}
			return arr
		}

		l := make(map[string]interface{})
		for _, v := range values {
			unflatMap(l, v.name[1:], v.Value)
		}
		return l
	}

	return values[0].Value
}
