package expr

import "reflect"

// F :
type F struct {
	Name   string
	Values []reflect.Value
}

func Field(name string, vals interface{}) (f F) {
	v := reflect.ValueOf(vals)
	k := v.Kind()
	if k != reflect.Slice && k != reflect.Array {
		panic("expr: invalid data type for Field")
	}
	f.Name = name
	length := v.Len()
	if length < 1 {
		panic("expr: empty values")
	}
	for i := 0; i < length; i++ {
		f.Values = append(f.Values, v.Index(i))
	}
	return
}
