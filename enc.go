package goloquent

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

var defaultRegistry *Registry

type encodeFunc func(reflect.Value) (interface{}, error)

type Registry struct {
	sync.Mutex
	typeEncoders map[reflect.Type]encodeFunc
	kindEncoders map[reflect.Kind]encodeFunc
}

func init() {
	defaultRegistry = NewRegistry()
	defaultRegistry.SetDefaultEncoders()
}

func NewRegistry() *Registry {
	return &Registry{
		typeEncoders: make(map[reflect.Type]encodeFunc),
		kindEncoders: make(map[reflect.Kind]encodeFunc),
	}
}

func (r *Registry) SetDefaultEncoders() {
	enc := DefaultEncoder{registry: r}
	r.SetTypeEncoder(reflect.TypeOf([]byte{}), enc.encodeByte)
	r.SetKindEncoder(reflect.String, enc.encodeString)
	r.SetKindEncoder(reflect.Bool, enc.encodeBool)
	r.SetKindEncoder(reflect.Int, enc.encodeInteger)
	r.SetKindEncoder(reflect.Int8, enc.encodeInteger)
	r.SetKindEncoder(reflect.Int16, enc.encodeInteger)
	r.SetKindEncoder(reflect.Int32, enc.encodeInteger)
	r.SetKindEncoder(reflect.Int64, enc.encodeInteger)
	r.SetKindEncoder(reflect.Uint, enc.encodeUInteger)
	r.SetKindEncoder(reflect.Uint8, enc.encodeUInteger)
	r.SetKindEncoder(reflect.Uint16, enc.encodeUInteger)
	r.SetKindEncoder(reflect.Uint32, enc.encodeUInteger)
	r.SetKindEncoder(reflect.Uint64, enc.encodeUInteger)
	r.SetKindEncoder(reflect.Float32, enc.encodeFloat)
	r.SetKindEncoder(reflect.Float64, enc.encodeFloat)
	r.SetKindEncoder(reflect.Ptr, enc.encodePtr)
	r.SetKindEncoder(reflect.Array, enc.encodeArray)
	r.SetKindEncoder(reflect.Slice, enc.encodeSlice)
}

func (r *Registry) SetTypeEncoder(t reflect.Type, f encodeFunc) {
	r.Lock()
	defer r.Unlock()
	r.typeEncoders[t] = f
}

func (r *Registry) SetKindEncoder(k reflect.Kind, f encodeFunc) {
	r.Lock()
	defer r.Unlock()
	r.kindEncoders[k] = f
}

func (r *Registry) EncodeValue(v reflect.Value) (interface{}, error) {
	if encoder, isOk := r.typeEncoders[v.Type()]; isOk {
		return encoder(v)
	}
	if encoder, isOk := r.kindEncoders[v.Kind()]; isOk {
		return encoder(v)
	}
	return nil, fmt.Errorf("unsupported data type: %v", v)
}

type DefaultEncoder struct {
	registry *Registry
}

func (enc DefaultEncoder) encodeString(v reflect.Value) (interface{}, error) {
	return v.String(), nil
}

func (enc DefaultEncoder) encodeByte(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return "null", nil
	}
	return base64.StdEncoding.EncodeToString(v.Bytes()), nil
}

func (enc DefaultEncoder) encodeInteger(v reflect.Value) (interface{}, error) {
	return v.Int(), nil
}

func (enc DefaultEncoder) encodeUInteger(v reflect.Value) (interface{}, error) {
	return v.Uint(), nil
}

func (enc DefaultEncoder) encodeBool(v reflect.Value) (interface{}, error) {
	return v.Bool(), nil
}

func (enc DefaultEncoder) encodeFloat(v reflect.Value) (interface{}, error) {
	return v.Float(), nil
}

func (enc DefaultEncoder) encodePtr(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return nil, nil
	}
	v = v.Elem()
	return enc.registry.EncodeValue(v)
}

func (enc DefaultEncoder) encodeArray(v reflect.Value) (interface{}, error) {
	return enc.encodeArrayOrSlice(v)
}

func (enc DefaultEncoder) encodeSlice(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return "null", nil
	}
	return enc.encodeArrayOrSlice(v)
}

func (enc DefaultEncoder) encodeArrayOrSlice(v reflect.Value) (interface{}, error) {
	blr := new(strings.Builder)
	blr.WriteByte('[')
	var (
		length = v.Len()
		vv     interface{}
		err    error
	)
	for i := 0; i < length; i++ {
		if i > 0 {
			blr.WriteByte(',')
		}
		vv, err = enc.registry.EncodeValue(v.Index(i))
		if err != nil {
			return nil, err
		}
		blr.WriteString(convertToString(vv))
	}
	blr.WriteByte(']')
	return blr.String(), nil
}

func convertToString(it interface{}) string {
	switch vi := it.(type) {
	case string:
		return strconv.Quote(vi)
	case bool:
		return strconv.FormatBool(vi)
	case int64:
		return strconv.FormatInt(vi, 10)
	case uint64:
		return strconv.FormatUint(vi, 10)
	case []byte:
		return strconv.Quote(string(vi))
	case nil:
		return "null"
	default:
		return "{}"
	}
}
