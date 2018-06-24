package db

import (
	"database/sql"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/si3nloong/goloquent"
)

// Connection :
func Connection(driver string) *goloquent.DB {
	x, isOk := connPool.Load(driver)
	if !isOk {
		panic(fmt.Errorf("goloquent: connection not found"))
	}
	pool := x.(map[string]*goloquent.DB)
	for _, v := range pool {
		return v
	}
	return nil
}

// Exec :
func Exec(stmt string, args ...interface{}) (sql.Result, error) {
	return defaultDB.Exec(stmt, args...)
}

// Table :
func Table(name string) *goloquent.Query {
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
func Ancestor(ancestorKey *datastore.Key) *goloquent.Query {
	return defaultDB.NewQuery().Ancestor(ancestorKey)
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

// RunInTransaction :
func RunInTransaction(cb goloquent.TransactionHandler) error {
	return defaultDB.RunInTransaction(cb)
}

// Truncate :
func Truncate(model interface{}) error {
	return defaultDB.Truncate(model)
}
