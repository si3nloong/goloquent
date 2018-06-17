package goloquent

import (
	"bytes"
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
	pkColumn         = "$Key"
	keyColumn        = "$Key"
	parentColumn     = "$Parent"
	softDeleteColumn = "$Deleted"
	keyDelimeter     = "/"
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
	c.Host = strings.TrimSpace(strings.ToLower(c.Host))
	c.Port = strings.TrimSpace(c.Port)
	c.Database = strings.TrimSpace(c.Database)
	c.UnixSocket = strings.TrimSpace(c.UnixSocket)
	if c.CharSet != nil {
		c.CharSet.Collation = strings.TrimSpace(c.CharSet.Collation)
		c.CharSet.Encoding = strings.TrimSpace(c.CharSet.Encoding)
	}
}

// Replacer :
type Replacer interface {
	Upsert(model interface{}, k ...*datastore.Key) error
	Save(model interface{}) error
}

// Client :
type Client struct {
	sqlCommon
	logger LogHandler
}

// Exec :
func (c Client) Exec(query string, args ...interface{}) (sql.Result, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(query)
	// go c.ConsoleLog(&Stmt{buf, args, nil})
	result, err := c.sqlCommon.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ConsoleLog :
func (c Client) ConsoleLog(stmt *Stmt) {
	if c.logger != nil {
		c.logger(stmt)
	}
}

// DB :
type DB struct {
	id      string
	driver  string
	name    string
	replica string
	conn    Client
	dialect Dialect
	omits   []string
}

// NewDB :
func NewDB(driver string, conn sqlCommon, dialect Dialect, logHandler LogHandler) *DB {
	client := Client{conn, logHandler}
	dialect.SetDB(client)
	return &DB{
		id:      fmt.Sprintf("%s:%d", driver, time.Now().UnixNano()),
		driver:  driver,
		name:    dialect.CurrentDB(),
		conn:    client,
		dialect: dialect,
	}
}

// clone a new connection
func (db *DB) clone() *DB {
	return &DB{
		id:      db.id,
		driver:  db.driver,
		name:    db.name,
		replica: fmt.Sprintf("%d", time.Now().Unix()),
		conn:    db.conn,
		dialect: db.dialect,
		// logger:  db.logger,
	}
}

// ID :
func (db *DB) ID() string {
	return db.id
}

// Raw :
func (db *DB) Raw(stmt string, args ...interface{}) *sql.Row {
	return newBuilder(db.NewQuery()).db.QueryRow(stmt, args...)
}

// Exec :
func (db *DB) Exec(stmt string, args ...interface{}) (sql.Result, error) {
	return newBuilder(db.NewQuery()).db.Exec(stmt, args...)
}

// NewQuery :
func (db *DB) NewQuery() *Query {
	return newQuery(db)
}

// Table :
func (db *DB) Table(name string) *Query {
	q := db.NewQuery()
	q.table = name
	return q
}

// Migrate :
func (db *DB) Migrate(model ...interface{}) error {
	return newBuilder(db.NewQuery()).migrate(model)
}

// Omit :
func (db *DB) Omit(fields ...string) Replacer {
	ff := newDictionary(fields)
	clone := db.clone()
	clone.omits = ff.keys()
	return clone
}

// Create :
func (db *DB) Create(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return newBuilder(db.NewQuery()).put(model, nil)
	}
	return newBuilder(db.NewQuery()).put(model, parentKey)
}

// Upsert :
func (db *DB) Upsert(model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return newBuilder(db.NewQuery().Omit(db.omits...)).upsert(model, nil)
	}
	return newBuilder(db.NewQuery().Omit(db.omits...)).upsert(model, parentKey)
}

// Save :
func (db *DB) Save(model interface{}) error {
	if err := checkSinglePtr(model); err != nil {
		return err
	}
	return newBuilder(db.NewQuery().Omit(db.omits...)).save(model)
}

// Delete :
func (db *DB) Delete(model interface{}) error {
	return newBuilder(db.NewQuery()).delete(model)
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
	return newBuilder(db.NewQuery()).truncate(table)
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

// Run :
func (db *DB) Run(query *Query) (*Iterator, error) {
	// return newBuilder(db.NewQuery()).run(new(Stmt))
	return nil, nil
}

// RunInTransaction :
func (db *DB) RunInTransaction(cb TransactionHandler) error {
	return newBuilder(db.NewQuery()).runInTransaction(cb)
}

// Close :
func (db *DB) Close() error {
	x, isOk := db.conn.sqlCommon.(*sql.DB)
	if !isOk {
		return nil
	}
	return x.Close()
}
