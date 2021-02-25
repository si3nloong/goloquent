package goloquent

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
)

type structDefinition struct {
	fields []structField
}

type structField struct {
	t   reflect.Type
	tag structTag
	def *structDefinition
}

type structTag struct {
	name string
	opts map[string]string
}

func parseStructTag(tags []string, f reflect.StructField) (t structTag) {
	t.name = f.Name
	t.opts = make(map[string]string)

	var paths []string
	for _, tag := range tags {
		v, ok := f.Tag.Lookup(tag)
		if !ok {
			continue
		}
		paths = strings.Split(v, ",")
		paths[0] = strings.TrimSpace(paths[0])
		if len(paths[0]) > 0 {
			t.name = paths[0]
			paths = paths[1:]
		}

		for _, p := range paths {
			t.opts[p] = p
		}
	}

	return
}

type entityCache struct {
	mu    sync.Mutex
	tag   []string
	cache map[reflect.Type]*structDefinition
}

var ec = &entityCache{
	tag:   []string{"db", "datastore", "goloquent"},
	cache: make(map[reflect.Type]*structDefinition),
}

func codecByType(t reflect.Type) *structDefinition {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	sd, ok := ec.cache[t]
	if ok {
		return sd
	}
	ec.cache[t] = getCodec(ec.tag, t)
	return ec.cache[t]
}

type typeQueue struct {
	t          reflect.Type
	parentPath string
}

func getCodec(tagName []string, t reflect.Type) (*structDefinition, error) {
	queue := []typeQueue{}
	queue = append(queue, typeQueue{elem(t), ""})

	sd := new(structDefinition)

	for len(queue) > 0 {
		q := queue[0]

		sf := structField{}
		for i := 0; i < q.t.NumField(); i++ {
			f := q.t.Field(i)

			// skip unexported fields
			if len(f.PkgPath) != 0 && !f.Anonymous {
				continue
			}
			log.Println(f)

			tag := parseStructTag(tagName, f)

			switch {
			case tag.name == "-":
				continue
			case !isValidFieldName(tag.name):
				return nil, fmt.Errorf("goloquent: struct tag has invalid field name: %q", tag.name)
			case isReserveFieldName(tag.name):
				return nil, fmt.Errorf("goloquent: struct tag has reserved field name: %q", tag.name)
				// case st.isPrimaryKey():
				// 	if sf.Type != typeOfPtrKey {
				// 		return nil, fmt.Errorf("goloquent: %s field on struct %v must be *datastore.Key", keyFieldName, ft)
				// 	}
			}

			sf.t = f.Type
			sf.tag = tag

			f = elem(f.Type)
			if f.Type.Kind() == reflect.Struct {

			}

			sd.fields = append(sd.fields, sf)
		}

		queue = queue[1:]
	}

	log.Println(sd)
	return sd, nil
}

func elem(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
