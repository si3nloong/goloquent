package goloquent

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/datastore"
)

type operator int

const (
	equal operator = iota
	notEqual
	lessThan
	lessEqual
	greaterThan
	greaterEqual
	like
	notLike
	in
	notIn
)

type sortDirection int

const (
	ascending sortDirection = iota
	descending
)

type order struct {
	field     string
	direction sortDirection
}

type locked int

// lock mode
const (
	ReadLock locked = iota + 1
	WriteLock
)

const (
	maxLimit     = 1000
	keyFieldName = "__key__"
)

// Filter :
type Filter struct {
	field    string
	operator operator
	value    interface{}
}

// Value :
func (f *Filter) Value() (interface{}, error) {
	if f.value == nil {
		return nil, nil
	}
	v, err := normalizeValue(f.value)
	if err != nil {
		return nil, err
	}
	return interfaceToValue(v)
}

func checkSinglePtr(it interface{}) error {
	v := reflect.ValueOf(it)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("goloquent: entity must be addressable")
	}
	if v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("goloquent: entity data type must be struct")
	}
	return nil
}

// Query :
type Query struct {
	db         *DB
	table      string
	ancestors  []*datastore.Key
	filters    []Filter
	projection []string
	orders     []order
	omits      []string
	limit      int32
	offset     int32
	distinctOn []string
	errs       []error
	relatives  []Relationship
	lockMode   locked
	isDebug    bool
	keyOnly    bool
	hasTrash   bool
}

func newQuery(db *DB) *Query {
	return &Query{
		db:     db,
		limit:  -1,
		offset: -1,
	}
}

func (q *Query) clone() *Query {
	c := *q
	return &c
}

func (q *Query) getError() error {
	if len(q.errs) > 0 {
		buf := new(bytes.Buffer)
		for _, err := range q.errs {
			buf.WriteString(fmt.Sprintf("%v", err))
		}
		return fmt.Errorf("%s", buf.String())
	}
	return nil
}

// Debug :
func (q *Query) Debug() *Query {
	q.isDebug = true
	return q
}

// Table :
func (q *Query) Table(name string) *Query {
	name = strings.TrimSpace(name)
	if name == "" {
		return q
	}
	q.table = name
	return q
}

// Select :
func (q *Query) Select(fields ...string) *Query {
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" || f == "*" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid selection value %v", f))
			return q
		}
		arr = append(arr, f)
	}
	// Primary key is always selected
	dict := newDictionary(append(q.projection, arr...))
	dict.delete(keyFieldName)
	dict.add(keyColumn)
	dict.add(parentColumn)
	q.projection = dict.keys()
	return q
}

// Find :
func (q *Query) Find(key *datastore.Key, model interface{}) error {
	if err := checkSinglePtr(model); err != nil {
		return nil
	}
	if key == nil || key.Incomplete() {
		return fmt.Errorf("goloquent: find action with invalid key value, %q", key)
	}
	q.Where(keyFieldName, "=", key).Limit(1)
	return q.db.stmt.get(q, model, true)
}

// First :
func (q *Query) First(model interface{}) error {
	if err := checkSinglePtr(model); err != nil {
		return nil
	}
	q.Limit(1)
	return q.db.stmt.get(q, model, false)
}

// Get :
func (q *Query) Get(model interface{}) error {
	return q.db.stmt.getMulti(q, model)
}

// Paginate :
func (q *Query) Paginate(p *Pagination, model interface{}) error {
	q = q.clone()
	if p.Limit > maxLimit {
		return fmt.Errorf("goloquent: limit overflow : %d, maximum limit : %d", p.Limit, maxLimit)
	}
	q.Limit(int(p.Limit) + 1).Order(keyFieldName)
	if p.Cursor != "" {
		c, err := datastore.DecodeKey(p.Cursor)
		if err != nil {
			return err
		}
		q.Where(keyFieldName, ">", c)
	}
	q.filters = append(q.filters, p.Filter...)
	q.Order(p.Sort...)
	return q.db.stmt.paginate(q, p, model)
}

// // Relative :
// func (q *Query) Relative(r Relationship) *Query {
// 	q.relatives = append(q.relatives, r)
// 	return q
// }

// DistinctOn :
func (q *Query) DistinctOn(fields ...string) *Query {
	// Primary key is always selected
	dict := newDictionary(append(q.distinctOn, fields...))
	dict.delete(keyFieldName)
	dict.add(keyColumn)
	dict.add(parentColumn)
	q.distinctOn = dict.keys()
	return q
}

// Ancestor :
func (q *Query) Ancestor(ancestor *datastore.Key) *Query {
	clone := q.clone()
	if ancestor.Incomplete() {
		clone.errs = append(clone.errs,
			fmt.Errorf("goloquent: ancestor key is incomplete, %v", ancestor))
		return q
	}
	clone.ancestors = append(clone.ancestors, ancestor)
	return clone
}

// Where :
func (q *Query) Where(field string, op string, value interface{}) *Query {
	field = strings.TrimSpace(field)
	op = strings.TrimSpace(op)

	var optr operator
	switch strings.ToLower(op) {
	case "=", "<=>", "eq":
		optr = equal
	case "!=", "<>", "ne":
		optr = notEqual
	case ">", "gt", "!<":
		optr = greaterThan
	case "<", "lt", "!>":
		optr = lessThan
	case ">=", "gte":
		optr = greaterEqual
	case "<=", "lte":
		optr = lessEqual
	case "like":
		optr = like
	case "nlike", "!like":
		optr = notLike
	case "in":
		optr = in
	case "nin", "!in":
		optr = notIn
	default:
		q.errs = append(q.errs, fmt.Errorf("goloquent: invalid operator %q", op))
		return q
	}

	q.filters = append(q.filters, Filter{
		field:    field,
		operator: optr,
		value:    value,
	})
	return q
}

// WhereIn :
func (q *Query) WhereIn(field string, v []interface{}) *Query {
	return q.Where(field, "in", v)
}

// WhereNotIn :
func (q *Query) WhereNotIn(field string, v []interface{}) *Query {
	return q.Where(field, "nin", v)
}

// WhereLike :
func (q *Query) WhereLike(field string, v interface{}) *Query {
	return q.Where(field, "like", v)
}

// WhereNotLike :
func (q *Query) WhereNotLike(field string, v interface{}) *Query {
	return q.Where(field, "nlike", v)
}

// Limit :
func (q *Query) Limit(limit int) *Query {
	q.limit = int32(limit)
	return q
}

// Offset :
func (q *Query) Offset(offset int) *Query {
	q.offset = int32(offset)
	return q
}

// Order :
func (q *Query) Order(fields ...string) *Query {
	if len(fields) <= 0 {
		return q
	}

	f := fields[0]
	name, dir := strings.TrimSpace(f), ascending
	if strings.HasPrefix(name, "+") {
		name, dir = strings.TrimSpace(name[1:]), ascending
	} else if strings.HasPrefix(name, "-") {
		name, dir = strings.TrimSpace(name[1:]), descending
	}

	q.orders = append(q.orders, order{
		direction: dir,
		field:     name,
	})
	return q
}

// Lock :
func (q *Query) Lock(mode locked) *Query {
	q.lockMode = mode
	return q
}

// RLock :
func (q *Query) RLock() *Query {
	q.lockMode = ReadLock
	return q
}

// WLock :
func (q *Query) WLock() *Query {
	q.lockMode = WriteLock
	return q
}

// Flush :
func (q *Query) Flush() error {
	if q.table == "" {
		return fmt.Errorf("goloquent: unable to perform delete without table name")
	}
	return q.db.stmt.deleteByQuery(q.clone())
}
