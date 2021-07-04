package goloquent

import (
	"reflect"
	"strings"
)

const (
	tagOmit     = "omitempty"
	tagUnsigned = "unsigned"
	tagLongtext = "longtext"
	tagFlatten  = "flatten"
	tagIndex    = "index"
)

type tag struct {
	name    string
	options map[string]string
}

func parseTag(sf reflect.StructField) (t tag) {
	var (
		paths []string
		name  string
	)

	t.name = sf.Name
	t.options = make(map[string]string)
	for _, val := range []string{"datastore", "goloquent"} {
		val = strings.TrimSpace(sf.Tag.Get(val))
		paths = strings.Split(val, ",")
		name = strings.TrimSpace(paths[0])
		if name != "" {
			t.name = name
		}

		paths = paths[1:]
		for _, path := range paths {
			val := strings.SplitN(path, ":", 2)
			t.options[val[0]] = val[1]
		}
	}
	return
}

func (t tag) Lookup(key string) (val string, ok bool) {
	val, ok = t.options[key]
	return
}

func (t tag) isPrimaryKey() bool {
	return t.name == keyFieldName
}

func (t tag) IsSkip() (ok bool) {
	return t.name == "-"
}

func (t tag) IsFlatten() (ok bool) {
	_, ok = t.options[tagFlatten]
	return
}

func (t tag) IsIndex() (ok bool) {
	_, ok = t.options[tagIndex]
	return
}

func (t tag) IsOmitEmpty() (ok bool) {
	_, ok = t.options[tagOmit]
	return
}

func (t tag) IsUnsigned() (ok bool) {
	_, ok = t.options[tagUnsigned]
	return
}

func (t tag) IsLongText() (ok bool) {
	_, ok = t.options[tagLongtext]
	return
}
