package goloquent

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/si3nloong/goloquent/qson"
)

type rawString string

// PaginateOption :
type PaginateOption struct {
}

const (
	defaultLimit = 100
)

// Pagination :
type Pagination struct {
	Cursor string
	Filter []Filter
	Sort   []string
	Limit  uint
	count  uint
}

// Reset :
func (p *Pagination) Reset() {
	pp := new(Pagination)
	pp.Limit = defaultLimit
	p = pp
}

// Count :
func (p *Pagination) Count() uint {
	return p.count
}

func base64Decode(str string) []byte {
	if (!strings.HasPrefix(str, `{`) && !strings.HasSuffix(str, `}`)) &&
		!strings.HasPrefix(str, `[`) && !strings.HasSuffix(str, `]`) {
		b, _ := base64.StdEncoding.DecodeString(str)
		return b
	}
	return []byte(str)
}

func umarshalQuery(it interface{}, str string) (interface{}, error) {
	str = strings.TrimSpace(str)

	var v interface{}
	switch it.(type) {
	case rawString:
		v = string(str)
	case []byte:
		v = base64Decode(str)
	case []string:
		arr := make([]string, 0)
		if err := json.Unmarshal(base64Decode(str), &arr); err != nil {
			return nil, err
		}
		v = arr
	case int:
		i, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		v = i
	case string:
		v = string(base64Decode(str))
	default:
		return nil, fmt.Errorf("unsupported data type : %v", reflect.TypeOf(it))
	}

	return v, nil
}

// ParseQuery :
func ParseQuery(query []byte, layout interface{}, option ...PaginateOption) (*Pagination, error) {
	query = bytes.TrimSpace(query)
	queryStr := string(query)
	p := new(Pagination)
	if queryStr == "" {
		p.Reset()
		return p, nil
	}

	l, err := url.ParseQuery(queryStr)
	if err != nil {
		return nil, err
	}

	data := make(map[string]interface{})
	data["filter"] = []byte(nil)
	data["cursor"] = rawString("")
	data["sort"] = []string(nil)
	data["limit"] = 100

	for k, v := range data {
		vv, isExist := l[k]
		if !isExist {
			continue
		}

		it, err := umarshalQuery(v, vv[0])
		if err != nil {
			return nil, err
		}
		data[k] = it
	}

	fields, err := qson.Parse(data["filter"].([]byte), layout)
	if err != nil {
		return nil, err
	}

	filters := make([]Filter, 0, len(fields))
	for _, f := range fields {
		var op operator
		switch f.Operator() {
		case "$eq":
			op = equal
		case "$ne":
			op = notEqual
		case "$gt":
			op = greaterThan
		case "$lt":
			op = lessThan
		case "$gte":
			op = greaterEqual
		case "$lte":
			op = lessEqual
		case "$like":
			op = like
		case "$nlike":
			op = notLike
		case "$in":
			op = in
		case "$nin":
			op = notIn
		default:
			return nil, fmt.Errorf("goloquent: unsupported operator %q", f.Operator())
		}

		ff := new(Filter)
		ff.field = f.QSON()
		ff.operator = op
		ff.value = f.Value()
		filters = append(filters, *ff)
	}

	return &Pagination{
		Cursor: data["cursor"].(string),
		Filter: filters,
		Sort:   []string(nil),
		Limit:  uint(data["limit"].(int64)),
	}, nil
}
