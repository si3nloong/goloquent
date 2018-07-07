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
	equalTo
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

func checkSinglePtr(it interface{}) error {
	v := reflect.ValueOf(it)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("goloquent: entity must be addressable")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct || isBaseType(v.Type()) {
		return fmt.Errorf("goloquent: entity data type must be struct")
	}
	return nil
}

type scope struct {
	table      string
	distinctOn []string
	projection []string
	omits      []string
	ancestors  []*datastore.Key
	filters    []Filter
	orders     []order
	limit      int32
	offset     int32
	errs       []error
	noScope    bool
	lockMode   locked
}

// Query :
type Query struct {
	db *DB
	scope
}

func newQuery(db *DB) *Query {
	return &Query{
		db: db.clone(),
		scope: scope{
			limit:  -1,
			offset: -1,
		},
	}
}

func (q *Query) clone() *Query {
	ss := q.scope
	return &Query{
		db:    q.db.clone(),
		scope: ss,
	}
}

func (q *Query) append(query *Query) *Query {
	q.scope.projection = append(q.scope.projection, query.scope.projection...)
	q.scope.filters = append(q.scope.filters, query.scope.filters...)
	q.scope.orders = append(q.scope.orders, query.scope.orders...)
	return q
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

// Select :
func (q *Query) Select(fields ...string) *Query {
	q = q.clone()
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid `Select` value %q", f))
			return q
		}
		arr = append(arr, f)
	}
	q.projection = append(q.projection, arr...)
	return q
}

// DistinctOn :
func (q *Query) DistinctOn(fields ...string) *Query {
	q = q.clone()
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" || f == "*" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid `DistinctOn` value %q", f))
			return q
		}
		arr = append(arr, f)
	}
	q.distinctOn = append(q.distinctOn, arr...)
	return q
}

// Omit :
func (q *Query) Omit(fields ...string) *Query {
	q = q.clone()
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" || f == "*" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid omit value %v", f))
			return q
		}
		arr = append(arr, f)
	}
	// Primary key cannot be omited
	dict := newDictionary(append(q.projection, arr...))
	dict.delete(keyFieldName)
	dict.delete(pkColumn)
	q.omits = dict.keys()
	return q
}

// Unscoped :
func (q *Query) Unscoped() *Query {
	q.noScope = true
	return q
}

// Find :
func (q *Query) Find(key *datastore.Key, model interface{}) error {
	if err := q.getError(); err != nil {
		return err
	}
	if err := checkSinglePtr(model); err != nil {
		return err
	}
	if key == nil || key.Incomplete() {
		return fmt.Errorf("goloquent: find action with invalid key value, %q", key)
	}
	q = q.Where(keyFieldName, "=", key).Limit(1)
	return newBuilder(q).get(model, true)
}

// First :
func (q *Query) First(model interface{}) error {
	q = q.clone()
	if err := q.getError(); err != nil {
		return err
	}
	if err := checkSinglePtr(model); err != nil {
		return err
	}
	q.Limit(1)
	return newBuilder(q).get(model, false)
}

// Get :
func (q *Query) Get(model interface{}) error {
	q = q.clone()
	if err := q.getError(); err != nil {
		return err
	}
	return newBuilder(q).getMulti(model)
}

// Paginate :
func (q *Query) Paginate(p *Pagination, model interface{}) error {
	if err := q.getError(); err != nil {
		return err
	}
	q = q.clone()
	if p.query != nil {
		q = q.append(p.query)
	}
	if p.Limit > maxLimit {
		return fmt.Errorf("goloquent: limit overflow : %d, maximum limit : %d", p.Limit, maxLimit)
	} else if p.Limit <= 0 {
		p.Limit = defaultLimit
	}
	q = q.Limit(int(p.Limit) + 1)
	if len(q.orders) > 0 {
		if q.orders[len(q.orders)-1].field != pkColumn {
			q = q.Order(pkColumn)
		}
	} else {
		q = q.Order(pkColumn)
	}
	return newBuilder(q).paginate(p, model)
}

// Ancestor :
func (q *Query) Ancestor(ancestor *datastore.Key) *Query {
	q = q.clone()
	if ancestor.Incomplete() {
		q.errs = append(q.errs, fmt.Errorf("goloquent: ancestor key is incomplete, %v", ancestor))
		return q
	}
	q.ancestors = append(q.ancestors, ancestor)
	return q
}

func (q *Query) where(field columner, op string, value interface{}) *Query {
	op = strings.TrimSpace(op)

	var optr operator
	switch strings.ToLower(op) {
	// TODO: safe equal
	// case "<=>", "==":
	// 	optr = equalTo
	case "=", "eq", "$eq":
		optr = equal
	case "!=", "<>", "ne", "$ne":
		optr = notEqual
	case ">", "!<", "gt", "$gt":
		optr = greaterThan
	case "<", "!>", "lt", "$lt":
		optr = lessThan
	case ">=", "gte", "$gte":
		optr = greaterEqual
	case "<=", "lte", "$lte":
		optr = lessEqual
	case "like", "$like":
		optr = like
	case "nlike", "!like", "$nlike":
		optr = notLike
	case "in", "$in":
		optr = in
	case "nin", "!in", "$nin":
		optr = notIn
	default:
		q.errs = append(q.errs, fmt.Errorf("goloquent: invalid operator %q", op))
		return q
	}

	q.filters = append(q.filters, Filter{
		columner: field,
		operator: optr,
		value:    value,
	})
	return q
}

// Where :
func (q *Query) Where(field string, op string, value interface{}) *Query {
	q = q.clone()
	field = strings.TrimSpace(field)
	return q.where(rawColumn{field}, op, value)
}

// WhereEqual :
func (q *Query) WhereEqual(field string, v interface{}) *Query {
	return q.Where(field, "=", v)
}

// WhereNotEqual :
func (q *Query) WhereNotEqual(field string, v interface{}) *Query {
	return q.Where(field, "!=", v)
}

// WhereNull :
func (q *Query) WhereNull(field string) *Query {
	return q.Where(field, "=", nil)
}

// WhereNotNull :
func (q *Query) WhereNotNull(field string) *Query {
	return q.Where(field, "<>", nil)
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
func (q *Query) WhereLike(field, v string) *Query {
	return q.Where(field, "like", v)
}

// WhereNotLike :
func (q *Query) WhereNotLike(field, v string) *Query {
	return q.Where(field, "nlike", v)
}

// WhereJSONEqual :
func (q *Query) WhereJSONEqual(field string, v interface{}) *Query {
	field = strings.TrimSpace(field)
	return q.where(jsonColumn{field}, "=", v)
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

// Order :
func (q *Query) Order(fields ...string) *Query {
	if len(fields) <= 0 {
		return q
	}

	for _, ff := range fields {
		q = q.clone()
		name, dir := strings.TrimSpace(ff), ascending
		if strings.HasPrefix(name, "+") {
			name, dir = strings.TrimSpace(name[1:]), ascending
		} else if strings.HasPrefix(name, "-") {
			name, dir = strings.TrimSpace(name[1:]), descending
		}

		q.orders = append(q.orders, order{
			field:     name,
			direction: dir,
		})
	}
	return q
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

// Update :
func (q *Query) Update(v interface{}) error {
	if err := q.getError(); err != nil {
		return err
	}
	q = q.Order(pkColumn)
	return newBuilder(q).updateMulti(v)
}

// Flush :
func (q *Query) Flush() error {
	if err := q.getError(); err != nil {
		return err
	}
	if q.table == "" {
		return fmt.Errorf("goloquent: unable to perform delete without table name")
	}
	return newBuilder(q).deleteByQuery()
}

// Scan :
func (q *Query) Scan(dest ...interface{}) error {
	return newBuilder(q).scan(dest...)
}
