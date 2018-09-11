package goloquent

import (
	"cloud.google.com/go/datastore"
)

// Table :
type Table struct {
	name string
	db   *DB
}

func (t *Table) newQuery() *Query {
	q := t.db.NewQuery()
	q.table = t.name
	return q
}

// Exists :
func (t *Table) Exists() bool {
	return t.db.dialect.HasTable(t.name)
}

// DropIfExists :
func (t *Table) DropIfExists() error {
	return newBuilder(t.newQuery()).dropTableIfExists(t.name)
}

// Truncate :
func (t *Table) Truncate() error {
	return newBuilder(t.newQuery()).truncate(t.name)
}

// // Rename :
// func (t *Table) Rename(name string) error {
// 	return newBuilder(t.newQuery()).renameTable(t.name, name)
// }

// AddIndex :
func (t *Table) AddIndex(fields ...string) error {
	return newBuilder(t.newQuery()).addIndex(fields, bTreeIdx)
}

// AddUniqueIndex :
func (t *Table) AddUniqueIndex(fields ...string) error {
	return newBuilder(t.newQuery()).addIndex(fields, uniqueIdx)
}

// Select :
func (t *Table) Select(fields ...string) *Query {
	return t.newQuery().Select(fields...)
}

// DistinctOn :
func (t *Table) DistinctOn(fields ...string) *Query {
	return t.newQuery().DistinctOn(fields...)
}

// Omit :
func (t *Table) Omit(fields ...string) *Query {
	return t.newQuery().Omit(fields...)
}

// Unscoped :
func (t *Table) Unscoped() *Query {
	return t.newQuery().Unscoped()
}

// Find :
func (t *Table) Find(key *datastore.Key, model interface{}) error {
	return t.newQuery().Find(key, model)
}

// First :
func (t *Table) First(model interface{}) error {
	return t.newQuery().First(model)
}

// Get :
func (t *Table) Get(model interface{}) error {
	return t.newQuery().Get(model)
}

// Paginate :
func (t *Table) Paginate(p *Pagination, model interface{}) error {
	return t.newQuery().Paginate(p, model)
}

// AnyOfAncestor :
func (t *Table) AnyOfAncestor(ancestors ...*datastore.Key) *Query {
	return t.newQuery().AnyOfAncestor(ancestors...)
}

// Ancestor :
func (t *Table) Ancestor(ancestor *datastore.Key) *Query {
	return t.newQuery().Ancestor(ancestor)
}

// Where :
func (t *Table) Where(field, op string, value interface{}) *Query {
	return t.newQuery().Where(field, op, value)
}

// WhereEqual :
func (t *Table) WhereEqual(field string, v interface{}) *Query {
	return t.newQuery().WhereEqual(field, v)
}

// WhereNotEqual :
func (t *Table) WhereNotEqual(field string, v interface{}) *Query {
	return t.newQuery().WhereNotEqual(field, v)
}

// WhereNull :
func (t *Table) WhereNull(field string) *Query {
	return t.newQuery().WhereNull(field)
}

// WhereNotNull :
func (t *Table) WhereNotNull(field string) *Query {
	return t.newQuery().WhereNotNull(field)
}

// WhereIn :
func (t *Table) WhereIn(field string, v []interface{}) *Query {
	return t.newQuery().WhereIn(field, v)
}

// WhereNotIn :
func (t *Table) WhereNotIn(field string, v []interface{}) *Query {
	return t.newQuery().WhereNotIn(field, v)
}

// WhereLike :
func (t *Table) WhereLike(field, v string) *Query {
	return t.newQuery().WhereLike(field, v)
}

// WhereNotLike :
func (t *Table) WhereNotLike(field, v string) *Query {
	return t.newQuery().WhereNotLike(field, v)
}

// WhereJSONEqual :
func (t *Table) WhereJSONEqual(field string, v interface{}) *Query {
	return t.newQuery().WhereJSONEqual(field, v)
}

// Lock :
func (t *Table) Lock(mode locked) *Query {
	return t.newQuery().Lock(mode)
}

// WLock :
func (t *Table) WLock() *Query {
	return t.newQuery().WLock()
}

// RLock :
func (t *Table) RLock() *Query {
	return t.newQuery().RLock()
}

// Order :
func (t *Table) Order(fields ...string) *Query {
	return t.newQuery().Order(fields...)
}

// Limit :
func (t *Table) Limit(limit int) *Query {
	return t.newQuery().Limit(limit)
}

// Offset :
func (t *Table) Offset(offset int) *Query {
	return t.newQuery().Offset(offset)
}

// ReplaceInto :
func (t *Table) ReplaceInto(table string) error {
	return t.newQuery().ReplaceInto(table)
}

// Update :
func (t *Table) Update(v interface{}) error {
	return t.newQuery().Update(v)
}

// Scan :
func (t *Table) Scan(dest ...interface{}) error {
	return t.newQuery().Scan(dest...)
}
