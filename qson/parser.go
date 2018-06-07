package qson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type direction int

const (
	ascending direction = iota
	descending
)

// Parser :
type Parser struct {
	codec map[string]*Property
}

// New :
func New(src interface{}) (*Parser, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	if v.Type().Kind() != reflect.Struct {
		return nil, fmt.Errorf("qson: invalid data type %v", v.Type())
	}
	return &Parser{
		codec: getProperty(v.Type()),
	}, nil
}

// Parse :
func (p *Parser) Parse(b []byte) ([]Field, error) {
	b = bytes.TrimSpace(b)
	if len(b) <= 0 || string(b) == `{}` {
		return nil, nil
	}

	l := make(map[string]interface{})
	if err := json.Unmarshal(b, &l); err != nil {
		return nil, fmt.Errorf("qson: unable to unmarshal query to json")
	}

	fields := make([]Field, 0)
	for k, v := range l {
		p, isValid := p.codec[k]
		if !isValid {
			return nil, fmt.Errorf("qson: invalid filter field %q", k)
		}

		name := p.QSON()
		switch vi := v.(type) {
		case map[string]interface{}:
			for op, vv := range vi {
				if !validOperator(op) {
					return nil, fmt.Errorf("qson: json key %q has invalid operator %q", k, op)
				}

				if op == in || op == nin {
					x, isOk := vv.([]interface{})
					if !isOk {
						return nil, fmt.Errorf("qson: json key %q has invalid value %v", k, vv)
					}

					arr := reflect.MakeSlice(reflect.SliceOf(p.typeOf), len(x), len(x))
					for i, xx := range x {
						it, err := convertToInterface(p.typeOf, xx)
						if err != nil {
							return nil, err
						}
						arr.Index(i).Set(reflect.ValueOf(it))
					}

					fields = append(fields, Field{name, op, arr.Interface()})
					continue
				}

				it, err := convertToInterface(p.typeOf, vv)
				if err != nil {
					return nil, err
				}

				fields = append(fields, Field{name, op, it})
			}
		default:
			it, err := convertToInterface(p.typeOf, vi)
			if err != nil {
				return nil, err
			}

			fields = append(fields, Field{name, eq, it})
		}
	}
	return fields, nil
}

// Sort :
type Sort struct {
	field string
	dir   direction
}

// Name :
func (s Sort) Name() string {
	return s.field
}

// IsAscending :
func (s Sort) IsAscending() bool {
	return s.dir == ascending
}

// ParseSort :
func (p *Parser) ParseSort(fields []string) ([]Sort, error) {
	sorts := make([]Sort, 0, len(fields))
	dict := make(map[string]bool)
	for _, ff := range fields {
		ff = strings.Trim(strings.TrimSpace(ff), `"`)
		dir := ascending
		if ff == "" {
			continue
		}
		if ff[0] == '-' {
			dir = descending
			ff = ff[1:]
		}
		c, isExist := p.codec[ff]
		if !isExist {
			return nil, fmt.Errorf("qson: invalid order field %q", ff)
		}
		name := c.QSON()
		if dict[name] {
			continue
		}
		sorts = append(sorts, Sort{name, dir})
		dict[name] = true
	}
	return sorts, nil
}
