package goloquent

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

// TransactionHandler :
type TransactionHandler func(*DB) error

// public constant variables :
const (
	keyColumn    = "$Key"
	parentColumn = "$Parent"
)

// Config :
type Config struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string
	IsSocket bool
}

// DB :
type DB struct {
	id   string
	name string
	stmt *Stmt
}

// NewDB :
func NewDB(driver string, conn sqlCommon, dialect Dialect) *DB {
	return &DB{
		id:   fmt.Sprintf("%s:%d", driver, time.Now().UnixNano()),
		name: dialect.CurrentDB(),
		stmt: &Stmt{db: conn, dialect: dialect},
	}
}

func (db *DB) clone() *DB {
	clone := *db
	return &clone
}

// ID :
func (db *DB) ID() string {
	return db.id
}

// NewQuery :
func (db *DB) NewQuery() *Query {
	return newQuery(db.clone())
}

// Table :
func (db *DB) Table(name string) *Query {
	return db.NewQuery().Table(name)
}

// Migrate :
func (db *DB) Migrate(model ...interface{}) error {
	return db.stmt.migrate(model)
}

// Create :
func (db *DB) Create(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return db.stmt.put(db.NewQuery(), model, nil)
	}
	return db.stmt.put(db.NewQuery(), model, parentKey)
}

// Upsert :
func (db *DB) Upsert(model interface{}, parentKey ...*datastore.Key) error {
	fmt.Println("debug :: ", parentKey, parentKey == nil)
	if parentKey == nil {
		return db.stmt.upsert(db.NewQuery(), model, nil)
	}
	return db.stmt.upsert(db.NewQuery(), model, parentKey)
}

// Save :
func (db *DB) Save(model interface{}) error {
	return db.stmt.update(db.NewQuery(), model)
}

// Delete :
func (db *DB) Delete(model interface{}) error {
	return db.stmt.delete(db.NewQuery(), model)
}

// Truncate :
func (db *DB) Truncate(model interface{}) error {
	var table string
	v := reflect.Indirect(reflect.ValueOf(model))

	switch v.Type().Kind() {
	case reflect.String:
		table = v.Interface().(string)
	case reflect.Struct:
		ety, err := newEntity(model)
		if err != nil {
			return err
		}
		table = ety.name
	default:
		return fmt.Errorf("goloquent: unsupported model")
	}

	if table == "" {
		return fmt.Errorf("goloquent: missing table name")
	}

	return db.stmt.truncate(table)
}

// Select :
func (db *DB) Select(fields ...string) *Query {
	return db.NewQuery().Select(fields...)
}

// Find :
func (db *DB) Find(key *datastore.Key, model interface{}) error {
	return db.NewQuery().Find(key, model)
}

// First :
func (db *DB) First(model interface{}) error {
	return db.NewQuery().First(model)
}

// Get :
func (db *DB) Get(model interface{}) error {
	return db.NewQuery().Get(model)
}

// Paginate :
func (db *DB) Paginate(p *Pagination, model interface{}) error {
	return db.NewQuery().Paginate(p, model)
}

// Where :
func (db *DB) Where(field string, operator string, value interface{}) *Query {
	return db.NewQuery().Where(field, operator, value)
}

// RunInTransaction :
func (db *DB) RunInTransaction(cb TransactionHandler) error {
	return db.stmt.runInTransaction(cb)
}

// Close :
func (db *DB) Close() error {
	x, isOk := db.stmt.db.(*sql.DB)
	if !isOk {
		return nil
	}
	return x.Close()
}
