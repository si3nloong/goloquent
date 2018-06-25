package goloquent

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/datastore"
)

var (
	typeOfByte       = reflect.TypeOf([]byte(nil))
	typeOfTime       = reflect.TypeOf(time.Time{})
	typeOfPtrKey     = reflect.TypeOf(&datastore.Key{})
	typeOfGeoPoint   = reflect.TypeOf(datastore.GeoPoint{})
	typeOfSoftDelete = reflect.TypeOf(SoftDelete(nil))
)

type field struct {
	tag
	names      []string
	parent     *field
	paths      []int
	sequence   []int
	typeOf     reflect.Type
	isPtrChild bool
	*StructCodec
}

func newField(st tag, parent *field, path []int, sequence []int, t reflect.Type, isPtr bool, s *StructCodec) field {
	return field{
		tag:         st,
		parent:      parent,
		paths:       path,
		sequence:    sequence,
		typeOf:      t,
		isPtrChild:  isPtr,
		StructCodec: s,
	}
}

func (f field) getFullPath() []field {
	fields := append(make([]field, 0), f)
	parent := f.parent
	for parent != nil {
		fields = append(fields, *parent)
		if parent.parent == nil {
			break
		}
		parent = parent.parent
	}
	return fields
}

func (f field) getRoot() *field {
	parent := f.parent
	if parent == nil {
		return &f
	}

	for parent != nil {
		if parent.parent == nil {
			break
		}
		parent = parent.parent
	}

	return parent
}

func (f field) isFlatten() bool {
	root := f.getRoot()
	return root.StructCodec != nil && f.tag.isFlatten()
}

func (f field) isSlice() bool {
	k := f.typeOf.Kind()
	return f.typeOf != typeOfByte && (k == reflect.Slice || k == reflect.Array)
}

// StructCodec :
type StructCodec struct {
	parentField *field
	value       reflect.Value
	fields      []field
}

func newStructCodec(v reflect.Value) *StructCodec {
	return &StructCodec{
		value: v,
	}
}

func (sc *StructCodec) findField(name string) (*field, error) {
	for _, f := range sc.fields {
		if f.name == name {
			return &f, nil
		}
	}
	return nil, fmt.Errorf("goloquent: struct code cannot find field, %q", name)
}

func isValidFieldName(name string) bool {
	if name == "" {
		return false
	}
	for _, s := range strings.Split(name, ".") {
		if s == "" {
			return false
		}
		first := true
		for _, c := range s {
			if first {
				first = false
				if c != '_' && !unicode.IsLetter(c) {
					return false
				}
			} else {
				if c != '_' && !unicode.IsLetter(c) && !unicode.IsDigit(c) {
					return false
				}
			}
		}
	}
	return true
}

func isReserveFieldName(name string) bool {
	m := map[string]bool{
		strings.ToLower(pkColumn):         true,
		strings.ToLower(softDeleteColumn): true,
	}
	return m[strings.ToLower(name)]
}

func isBaseType(t reflect.Type) bool {
	k := t.Kind()
	switch true {
	case k == reflect.String:
		return true
	case t == typeOfByte:
		return true
	case k == reflect.Bool:
		return true
	case k == reflect.Uint, k == reflect.Uint8, k == reflect.Uint16, k == reflect.Uint32, k == reflect.Uint64:
		return true
	case k == reflect.Int, k == reflect.Int8, k == reflect.Int16, k == reflect.Int32, k == reflect.Int64:
		return true
	case k == reflect.Float32, k == reflect.Float64:
		return true
	case t == typeOfPtrKey || t == typeOfTime || t == typeOfGeoPoint:
		return true
	case t == typeOfSoftDelete:
		return true
	}
	return false
}

type structScan struct {
	path        []int
	sequence    []int
	typeOf      reflect.Type
	field       *field
	isPtrChild  bool
	StructCodec *StructCodec
}

func getStructCodec(it interface{}) (*StructCodec, error) {
	v := reflect.Indirect(reflect.ValueOf(it))
	rt := v.Type()
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("goloquent: invalid %q", rt.String())
	}

	structs := newStructCodec(v)
	structScans := append(make([]structScan, 0), structScan{nil, nil, rt, nil, false, structs})
	for len(structScans) > 0 {
		first := structScans[0]
		st := first.typeOf

		fields := first.StructCodec.fields
		for i := 0; i < st.NumField(); i++ {
			sf := st.Field(i)

			// Skip if not anonymous private property
			isExported := (sf.PkgPath == "")
			if !isExported && !sf.Anonymous {
				continue
			}

			ft := sf.Type
			st := newTag(sf)

			switch {
			case st.isSkip():
				continue
			case st.isPrimaryKey():
				if sf.Type != typeOfPtrKey {
					return nil, fmt.Errorf("goloquent: %s field on struct %v must be *datastore.Key", keyFieldName, ft)
				}
			case !isValidFieldName(st.name):
				return nil, fmt.Errorf("goloquent: struct tag has invalid field name: %q", st.name)
			case isReserveFieldName(st.name):
				return nil, fmt.Errorf("goloquent: struct tag has reserved field name: %q", st.name)
			}

			if ft == typeOfSoftDelete {
				st.name = softDeleteColumn
			}

			seq := append(first.sequence, i)
			k := ft.Kind()
			if isBaseType(ft) {
				fields = append(fields, newField(st, first.field, append(first.path, i), seq, ft, first.isPtrChild, nil))
				continue
			}

			isPtr := false
			switch {
			case k == reflect.Slice, k == reflect.Array:
				// isSlice = true
				elem := ft.Elem()
				switch {
				case st.isPrimaryKey():
					return nil, fmt.Errorf("goloquent: nested primary key in slice is not allow")
				case elem.Kind() == reflect.Interface:
					fallthrough
				case isBaseType(elem):
					fields = append(fields, newField(st, first.field, append(first.path, i), seq, sf.Type, first.isPtrChild, nil))
					continue
				case elem.Kind() == reflect.Ptr:
					isPtr = true
					if isBaseType(elem.Elem()) {
						fields = append(fields, newField(st, first.field, append(first.path, i), seq, sf.Type, first.isPtrChild, nil))
						continue
					}
					elem = elem.Elem()
					fallthrough
				default:
					if elem.Kind() == reflect.Struct {
						sc := newStructCodec(reflect.New(ft))
						f := newField(st, first.field, append(first.path, i), seq, sf.Type, first.isPtrChild, sc)
						fields = append(fields, f)
						structScans = append(structScans, structScan{nil, seq, elem, &f, isPtr, sc})
						continue
					}
				}

				return nil, fmt.Errorf("goloquent: struct has invalid data type %v", ft)
			case k == reflect.Ptr:
				isPtr = true
				ft = ft.Elem()
				switch {
				case isBaseType(ft) && ft != typeOfByte:
					fields = append(fields, newField(st, first.field, append(first.path, i), seq, sf.Type, first.isPtrChild, nil))
					continue
				case ft.Kind() == reflect.Struct:
				default:
					return nil, fmt.Errorf("goloquent: pointer has invalid data type %q", ft.String())
				}
				fallthrough
			case k == reflect.Struct:
				if sf.Anonymous {
					if !isExported {
						continue
					}
					structScans = append(structScans, structScan{append(first.path, i), seq, ft, first.field, isPtr, first.StructCodec})
					continue
				}

				sc := newStructCodec(reflect.New(ft))
				f := newField(st, first.field, []int{i}, seq, sf.Type, first.isPtrChild, sc)
				fields = append(fields, f)
				sc.parentField = &f
				// reset the position when it's another struct
				structScans = append(structScans, structScan{nil, seq, ft, &f, isPtr, sc})
				continue
			default:
				return nil, fmt.Errorf("goloquent: invalid %q", ft.String())
			}
		}

		// Sort the column follow by the sequence of struct property
		sort.Slice(fields, func(i, j int) bool {
			return compareVersion(strings.Trim(strings.Join(strings.Fields(fmt.Sprint(fields[i].sequence)), "."), "[]"),
				strings.Trim(strings.Join(strings.Fields(fmt.Sprint(fields[j].sequence)), "."), "[]")) > 0
			// return fields[i].sequence[0] < fields[j].sequence[0]
		})

		first.StructCodec.fields = fields
		structScans = structScans[1:] // unshift item
	}

	return structs, nil
}

func initAny(v reflect.Value) reflect.Value {
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(t.Elem()))
		}
	}
	return v
}

func mustGetField(v reflect.Value, f field) reflect.Value {
	v = initAny(v)
	for _, p := range f.paths {
		v = reflect.Indirect(v).Field(p)
	}
	return v
}

// FieldByIndex panic when the path has nil value in between (*type),
// however getFieldByIndex will traverse Field by Field to check whether the value is valid
// and it will return zero if the subsequent field is zero
func getFieldByIndex(v reflect.Value, path []int) reflect.Value {
	for _, p := range path {
		v = v.Field(p)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Zero(v.Type())
			}
		}
	}
	return v
}

// initialize empty slice when the slice is nil and addressable
// json.Marshal likely to interpret as null if the slice have no initial value
func initSlice(v reflect.Value) reflect.Value {
	v = reflect.Indirect(v)
	if v.IsNil() {
		s := reflect.MakeSlice(reflect.SliceOf(v.Type().Elem()), 0, 0)
		if v.CanSet() {
			v.Set(s)
			return v
		}
		return s
	}
	return v
}
