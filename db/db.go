package db

import (
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/si3nloong/goloquent"
)

// Connection :
func Connection(driver string) *goloquent.DB {
	if _, isOk := connPool.Load(driver); !isOk {
		panic(fmt.Errorf("goloquent: connection not found"))
	}
	return nil
}

// Ancestor :
func Ancestor(ancestorKey *datastore.Key) *goloquent.Query {
	return defaultDB.NewQuery().Ancestor(ancestorKey)
}

// DistinctOn :
func DistinctOn(fields ...string) *goloquent.Query {
	return defaultDB.NewQuery().DistinctOn(fields...)
}

// Table :
func Table(name string) *goloquent.Query {
	return defaultDB.Table(name)
}

// Migrate :
func Migrate(model ...interface{}) error {
	return defaultDB.Migrate(model...)
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

// Where :
func Where(field string, operator string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().Where(field, operator, value)
}

// RunInTransaction :
func RunInTransaction(cb goloquent.TransactionHandler) error {
	return defaultDB.RunInTransaction(cb)
}

// Truncate :
func Truncate(model interface{}) error {
	return defaultDB.Truncate(model)
}
