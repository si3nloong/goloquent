package db

import (
	"database/sql"
	"fmt"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/si3nloong/goloquent"
)

// Connection :
func Connection(driver string) *goloquent.DB {
	driver = strings.TrimSpace(driver)
	paths := strings.SplitN(driver, ":", 2)
	if len(paths) != 2 {
		panic(fmt.Errorf("goloquent: invalid connection name %q", driver))
	}
	x, exists := connPool.Load(driver)
	if !exists {
		panic(fmt.Errorf("goloquent: connection not found"))
	}
	pool := x.(map[string]*goloquent.DB)
	for k, v := range pool {
		if k == paths[1] {
			return v
		}
		// return v
	}
	return nil
}

// Query :
func Query(stmt string, args ...interface{}) (*sql.Rows, error) {
	return defaultDB.Query(stmt, args...)
}

// Exec :
func Exec(stmt string, args ...interface{}) (sql.Result, error) {
	return defaultDB.Exec(stmt, args...)
}

// Table :
func Table(name string) *goloquent.Table {
	return defaultDB.Table(name)
}

// Migrate :
func Migrate(model ...interface{}) error {
	return defaultDB.Migrate(model...)
}

// Omit :
func Omit(fields ...string) goloquent.Replacer {
	return defaultDB.Omit(fields...)
}

// Create :
func Create(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return defaultDB.Create(model)
	}
	return defaultDB.Create(model, parentKey...)
}

// Upsert :
func Upsert(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return defaultDB.Upsert(model)
	}
	return defaultDB.Upsert(model, parentKey...)
}

// Delete :
func Delete(model interface{}) error {
	return defaultDB.Delete(model)
}

// Destroy :
func Destroy(model interface{}) error {
	return defaultDB.Destroy(model)
}

// Save :
func Save(model interface{}) error {
	return defaultDB.Save(model)
}

// Find :
func Find(key *datastore.Key, model interface{}) error {
	return defaultDB.Find(key, model)
}

// First :
func First(model interface{}) error {
	return defaultDB.First(model)
}

// Get :
func Get(model interface{}) error {
	return defaultDB.Get(model)
}

// Paginate :
func Paginate(p *goloquent.Pagination, model interface{}) error {
	return defaultDB.Paginate(p, model)
}

// NewQuery :
func NewQuery() *goloquent.Query {
	return defaultDB.NewQuery()
}

// Select :
func Select(fields ...string) *goloquent.Query {
	return defaultDB.Select(fields...)
}

// Ancestor :
func Ancestor(ancestor *datastore.Key) *goloquent.Query {
	return defaultDB.NewQuery().Ancestor(ancestor)
}

// AnyOfAncestor :
func AnyOfAncestor(ancestors ...*datastore.Key) *goloquent.Query {
	return defaultDB.NewQuery().AnyOfAncestor(ancestors...)
}

// Unscoped :
func Unscoped() *goloquent.Query {
	return defaultDB.NewQuery().Unscoped()
}

// DistinctOn :
func DistinctOn(fields ...string) *goloquent.Query {
	return defaultDB.NewQuery().DistinctOn(fields...)
}

// Where :
func Where(field string, operator string, value interface{}) *goloquent.Query {
	return defaultDB.Where(field, operator, value)
}

// WhereEqual :
func WhereEqual(field string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().WhereEqual(field, value)
}

// WhereNotEqual :
func WhereNotEqual(field string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().WhereNotEqual(field, value)
}

// WhereNull :
func WhereNull(field string) *goloquent.Query {
	return defaultDB.NewQuery().WhereNull(field)
}

// WhereNotNull :
func WhereNotNull(field string) *goloquent.Query {
	return defaultDB.NewQuery().WhereNotNull(field)
}

// WhereJSON :
func WhereJSON(field string, operator string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().WhereJSON(field, operator, value)
}

// OrderBy :
func OrderBy(fields ...interface{}) *goloquent.Query {
	return defaultDB.NewQuery().OrderBy(fields...)
}

// Limit :
func Limit(limit int) *goloquent.Query {
	return defaultDB.NewQuery().Limit(limit)
}

// Offset :
func Offset(offset int) *goloquent.Query {
	return defaultDB.NewQuery().Offset(offset)
}

// RunInTransaction :
func RunInTransaction(cb goloquent.TransactionHandler) error {
	return defaultDB.RunInTransaction(cb)
}

// Truncate :
func Truncate(model ...interface{}) error {
	return defaultDB.Truncate(model...)
}
