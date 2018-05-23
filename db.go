package goloquent

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

// TransactionHandler :
type TransactionHandler func(*DB) error

// LogHandler :
type LogHandler func(*Stmt)

// public constant variables :
const (
	keyColumn    = "$Key"
	parentColumn = "$Parent"
	keyDelimeter = "/"
)

// CommonError :
var (
	ErrNoSuchEntity = fmt.Errorf("goloquent: entity not found")
)

// Config :
type Config struct {
	Username   string
	Password   string
	Host       string
	Port       string
	Database   string
	UnixSocket string
	CharSet    *CharSet
	Logger     LogHandler
}

func (c Config) trimSpace() {
	c.Username = strings.TrimSpace(c.Username)
	c.Host = strings.TrimSpace(c.Host)
	c.Port = strings.TrimSpace(c.Port)
	c.Database = strings.TrimSpace(c.Database)
	c.UnixSocket = strings.TrimSpace(c.UnixSocket)
	if c.CharSet != nil {
		c.CharSet.Collation = strings.TrimSpace(c.CharSet.Collation)
		c.CharSet.Encoding = strings.TrimSpace(c.CharSet.Encoding)
	}
}

// Creator :
type Creator interface {
	Create(model interface{}, k ...*datastore.Key) error
	Upsert(model interface{}, k ...*datastore.Key) error
	Save(model interface{}) error
}

// DB :
type DB struct {
	driver  string
	id      string
	name    string
	replica string
	conn    sqlCommon
	dialect Dialect
	logger  LogHandler
	query   *Query
}

// NewDB :
func NewDB(driver string, conn sqlCommon, dialect Dialect, logHandler LogHandler) *DB {
	dbName := dialect.CurrentDB()
	return &DB{
		driver:  driver,
		id:      fmt.Sprintf("%s:%d", driver, time.Now().UnixNano()),
		name:    dbName,
		conn:    conn,
		dialect: dialect,
		logger:  logHandler,
	}
}

// clone a new connection
func (db *DB) clone() *DB {
	return &DB{
		driver:  db.driver,
		id:      db.id,
		name:    db.name,
		replica: fmt.Sprintf("%d", time.Now().Unix()),
		conn:    db.conn,
		dialect: db.dialect,
		logger:  db.logger,
	}
}

// ID :
func (db *DB) ID() string {
	return db.id
}

// Raw :
func (db *DB) Raw(stmt string, args ...interface{}) *sql.Row {
	return newBuilder(db).db.QueryRow(stmt, args...)
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
	return newBuilder(db).migrate(model)
}

// Omit :
func (db *DB) Omit(fields ...string) Creator {
	clone := db.clone()
	// clone.query = db.query.Omit(fields...)
	return clone
}

// Create :
func (db *DB) Create(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return newBuilder(db).put(db.NewQuery(), model, nil)
	}
	return newBuilder(db).put(db.NewQuery(), model, parentKey)
}

// Upsert :
func (db *DB) Upsert(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return newBuilder(db).upsert(db.NewQuery(), model, nil)
	}
	return newBuilder(db).upsert(db.NewQuery(), model, parentKey)
}

// Save :
func (db *DB) Save(model interface{}) error {
	if err := checkSinglePtr(model); err != nil {
		return err
	}
	return newBuilder(db).save(db.NewQuery(), model)
}

// Delete :
func (db *DB) Delete(model interface{}) error {
	return newBuilder(db).delete(db.NewQuery(), model)
}

// Truncate :
func (db *DB) Truncate(model interface{}) error {
	var table string
	v := reflect.Indirect(reflect.ValueOf(model))
	switch v.Type().Kind() {
	case reflect.String:
		table = v.String()
	case reflect.Struct:
		e, err := newEntity(model)
		if err != nil {
			return err
		}
		table = e.name
	default:
		return fmt.Errorf("goloquent: unsupported model")
	}
	if table == "" {
		return fmt.Errorf("goloquent: missing table name")
	}
	return newBuilder(db).truncate(table)
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

// // Run :
// func (db *DB) Run(query *Query) (*Iterator, error) {
// 	return new(Iterator), nil
// }

// RunInTransaction :
func (db *DB) RunInTransaction(cb TransactionHandler) error {
	return newBuilder(db).runInTransaction(cb)
}

// Close :
func (db *DB) Close() error {
	x, isOk := db.conn.(*sql.DB)
	if !isOk {
		return nil
	}
	return x.Close()
}

// Table :
type Table struct {
	db *DB
}

// Create :
func (t *Table) Create(model interface{}) error {
	return t.db.Create(model)
}

// Delete :
func (t *Table) Delete(model interface{}) error {
	return t.db.Delete(model)
}

// Where :
func (t *Table) Where(field, operator string, value interface{}) *Query {
	return t.db.Where(field, operator, value)
}
