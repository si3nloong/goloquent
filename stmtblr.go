package goloquent

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/si3nloong/goloquent/expr"
)

var stmtRegistry *StmtRegistry

type Writer interface {
	io.Writer
	io.StringWriter
	io.ByteWriter
}

type writerFunc func(Writer, reflect.Value) ([]interface{}, error)

type StmtRegistry struct {
	sync.Mutex
	typeEncoders map[reflect.Type]writerFunc
	kindEncoders map[reflect.Kind]writerFunc
}

func init() {
	stmtRegistry = NewStmtRegistry()
	stmtRegistry.SetDefaultEncoders()
}

func NewStmtRegistry() *StmtRegistry {
	return &StmtRegistry{
		typeEncoders: make(map[reflect.Type]writerFunc),
		kindEncoders: make(map[reflect.Kind]writerFunc),
	}
}

func (r *StmtRegistry) SetDefaultEncoders() {
	enc := DefaultStmtEncoder{registry: defaultRegistry}
	r.SetTypeEncoder(reflect.TypeOf(expr.F{}), enc.encodeField)
	r.SetTypeEncoder(reflect.TypeOf(expr.Sort{}), enc.encodeSort)
	r.SetKindEncoder(reflect.String, enc.encodeString)
}

func (r *StmtRegistry) SetTypeEncoder(t reflect.Type, f writerFunc) {
	r.Lock()
	defer r.Unlock()
	r.typeEncoders[t] = f
}

func (r *StmtRegistry) SetKindEncoder(k reflect.Kind, f writerFunc) {
	r.Lock()
	defer r.Unlock()
	r.kindEncoders[k] = f
}

func (r *StmtRegistry) BuildStatement(w Writer, v reflect.Value) ([]interface{}, error) {
	if encoder, isOk := r.typeEncoders[v.Type()]; isOk {
		return encoder(w, v)
	}
	if encoder, isOk := r.kindEncoders[v.Kind()]; isOk {
		return encoder(w, v)
	}
	return nil, fmt.Errorf("unsupported data type: %v", v)
}

type DefaultStmtEncoder struct {
	registry *Registry
}

func (enc DefaultStmtEncoder) encodeString(w Writer, v reflect.Value) ([]interface{}, error) {
	w.WriteString("`" + v.String() + "`")
	return nil, nil
}

func (enc DefaultStmtEncoder) encodeSort(w Writer, v reflect.Value) ([]interface{}, error) {
	x := v.Interface().(expr.Sort)
	w.WriteString("`" + x.Name + "`")
	if x.Direction == expr.Descending {
		w.WriteString(" DESC")
	}
	return nil, nil
}

func (enc DefaultStmtEncoder) encodeField(w Writer, v reflect.Value) ([]interface{}, error) {
	x, isOk := v.Interface().(expr.F)
	if !isOk {
		return nil, errors.New("invalid data type")
	}
	w.WriteString("FIELD")
	w.WriteByte('(')
	w.WriteString("`" + x.Name + "`")
	vals := make([]interface{}, 0)
	for _, vv := range x.Values {
		w.WriteByte(',')
		w.WriteString(variable)
		it, err := enc.registry.EncodeValue(vv)
		if err != nil {
			return nil, err
		}
		vals = append(vals, it)
	}
	w.WriteByte(')')
	return vals, nil
}
