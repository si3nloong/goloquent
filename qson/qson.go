package qson

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

func isBaseType(t reflect.Type) bool {
	return t == typeOfTime
}

// Property :
type Property struct {
	key    string
	name   string
	typeOf reflect.Type
	tag    reflect.StructTag
}

func (p *Property) getName(name string) string {
	n := strings.Split(p.tag.Get(name), ",")[0]
	n = strings.TrimSpace(n)
	if n == "" {
		return p.key
	}
	return n
}

// JSON :
func (p *Property) JSON() string {
	return p.getName("json")
}

// QSON :
func (p *Property) QSON() string {
	return p.getName("qson")
}

// Tag :
func (p *Property) Tag() reflect.StructTag {
	return p.tag
}

// Name :
func (p *Property) Name() string {
	return p.name
}

// Field :
type Field struct {
	name     string
	operator string
	value    interface{}
}

// Name :
func (f Field) Name() string {
	return f.name
}

// Operator :
func (f Field) Operator() string {
	return f.operator
}

// Value :
func (f Field) Value() interface{} {
	return f.value
}

const (
	eq    = "$eq"
	ne    = "$ne"
	not   = "$not"
	gt    = "$gt"
	gte   = "$gte"
	lt    = "$lt"
	lte   = "$lte"
	like  = "$like"
	nlike = "$nlike"
	in    = "$in"
	nin   = "$nin"
)

func validOperator(op string) (isOk bool) {
	return op == eq || op == ne || op == not ||
		op == gt || op == gte || op == lt || op == lte ||
		op == like || op == nlike ||
		op == in || op == nin
}

var (
	typeOfByte = reflect.TypeOf([]byte(nil))
	typeOfTime = reflect.TypeOf(time.Time{})
)

func convertToInterface(t reflect.Type, v interface{}) (interface{}, error) {
	var it interface{}

	switch t {
	case typeOfByte:
		x, isOk := v.(string)
		if !isOk {
			return nil, unmatchDataType(t, x)
		}
		it = []byte(x)
	case typeOfTime:
		x, isOk := v.(string)
		if !isOk {
			return nil, unmatchDataType(t, x)
		}
		vv, err := time.Parse(time.RFC3339, x)
		if err != nil {
			return nil, fmt.Errorf("qson: unable to convert %s to %v", x, t)
		}
		it = vv
	default:
		switch t.Kind() {
		case reflect.Ptr:
			if v == nil {
				return reflect.Zero(t).Interface(), nil
			}
			x, err := convertToInterface(t.Elem(), v)
			if err != nil {
				return nil, err
			}
			vv := reflect.ValueOf(x)
			vi := reflect.New(vv.Type())
			vi.Elem().Set(vv)
			it = vi.Interface()
		case reflect.String:
			x, isOk := v.(string)
			if !isOk {
				return nil, unmatchDataType(t, x)
			}
			it = x
		case reflect.Bool:
			x, isOk := v.(bool)
			if !isOk {
				return nil, unmatchDataType(t, x)
			}
			it = x
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			x, isOk := v.(float64)
			if !isOk {
				return nil, unmatchDataType(t, v)
			}
			if x < 0 {
				return nil, fmt.Errorf("qson: %s value has negative value, %v", t.Kind(), x)
			}
			v := reflect.New(t).Elem()
			if v.OverflowUint(uint64(x)) {
				return nil, fmt.Errorf("qson: %s value overflow, %v", t.Kind(), x)
			}
			v.SetUint(uint64(x))
			it = v.Interface()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			x, isOk := v.(float64)
			if !isOk {
				return nil, unmatchDataType(t, v)
			}
			v := reflect.New(t).Elem()
			if v.OverflowInt(int64(x)) {
				return nil, fmt.Errorf("qson: %s value overflow, %v", t.Kind(), x)
			}
			v.SetInt(int64(x))
			it = v.Interface()
		case reflect.Float32, reflect.Float64:
			x, isOk := v.(float64)
			if !isOk {
				return nil, unmatchDataType(t, x)
			}
			v := reflect.New(t).Elem()
			if v.OverflowFloat(x) {
				return nil, fmt.Errorf("qson: %s value overflow, %v", t.Kind(), x)
			}
			v.SetFloat(x)
			it = v.Interface()
		case reflect.Slice, reflect.Array:
			x, isOk := v.([]interface{})
			if !isOk {
				return nil, unmatchDataType(t, x)
			}
			for i, xx := range x {
				var err error
				x[i], err = convertToInterface(t.Elem(), xx)
				if err != nil {
					return nil, err
				}
			}
			it = x

		default:
			return nil, fmt.Errorf("qson: unsupported data type %v", t)
		}
	}

	return it, nil
}

func unmatchDataType(o reflect.Type, p interface{}) error {
	return fmt.Errorf("qson: unmatched data type of original, %v versus %v", o, reflect.TypeOf(p))
}

type structScan struct {
	name   []string
	typeOf reflect.Type
}

func getProperty(t reflect.Type) map[string]*Property {
	scans := append(make([]*structScan, 0), &structScan{nil, t})
	props := make(map[string]*Property)

	for len(scans) > 0 {
		first := scans[0]
		t := first.typeOf
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			// Skip if not anonymous private property
			isExported := (f.PkgPath == "")
			if !isExported && !f.Anonymous {
				continue
			}

			name := strings.Split(f.Tag.Get("json"), ",")[0]
			qson := strings.Split(f.Tag.Get("qson"), ",")[0]
			if name == "-" || qson == "-" {
				continue
			}

			if name == "" {
				name = f.Name
			}

			if f.Type.Kind() == reflect.Struct && !isBaseType(f.Type) {
				if f.Anonymous {
					if !isExported {
						continue
					}
					scans = append(scans, &structScan{first.name, f.Type})
					continue
				}
				scans = append(scans, &structScan{append(first.name, name), f.Type})
				continue
			}

			name = strings.Join(append(first.name, name), ".")
			p := &Property{
				key:    f.Name,
				name:   name,
				typeOf: f.Type,
				tag:    f.Tag,
			}

			props[name] = p
		}

		scans = scans[1:] // unshift
	}

	return props
}
