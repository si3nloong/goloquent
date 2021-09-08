package goloquent

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

var isPkSimple = true

// SetPKSimple :
func SetPKSimple(flag bool) {
	isPkSimple = flag
}

// TransactionHandler :
type TransactionHandler func(*DB) error

// LogHandler :
type LogHandler func(*Stmt)

// public constant variables :
const (
	pkLen            = 512
	pkColumn         = "$Key"
	softDeleteColumn = "$Deleted"
	keyDelimeter     = "/"
)

// CommonError :
var (
	ErrNoSuchEntity  = fmt.Errorf("goloquent: entity not found")
	ErrInvalidCursor = fmt.Errorf("goloquent: invalid cursor")
)

// Config :
type Config struct {
	Username   string
	Password   string
	Host       string
	Port       string
	Database   string
	UnixSocket string
	TLSConfig  string
	CharSet    *CharSet
	Logger     LogHandler
}

// Normalize :
func (c *Config) Normalize() {
	c.Username = strings.TrimSpace(c.Username)
	c.Host = strings.TrimSpace(strings.ToLower(c.Host))
	c.Port = strings.TrimSpace(c.Port)
	c.Database = strings.TrimSpace(c.Database)
	c.UnixSocket = strings.TrimSpace(c.UnixSocket)
	c.TLSConfig = strings.TrimSpace(c.TLSConfig)
	if c.CharSet != nil && c.CharSet.Encoding != "" && c.CharSet.Collation != "" {
		c.CharSet.Collation = strings.TrimSpace(c.CharSet.Collation)
		c.CharSet.Encoding = strings.TrimSpace(c.CharSet.Encoding)
	} else {
		charset := utf8mb4CharSet
		c.CharSet = &charset
	}
}

// Replacer :
type Replacer interface {
	Upsert(model interface{}, k ...*datastore.Key) error
	Save(model interface{}) error
}

// Client :
type Client struct {
	driver string
	sqlCommon
	CharSet
	dialect Dialect
	logger  LogHandler
}

func (c Client) consoleLog(s *Stmt) {
	if c.logger != nil {
		c.logger(s)
	}
}

func (c *Client) compileStmt(query string, args ...interface{}) *Stmt {
	buf := new(bytes.Buffer)
	buf.WriteString(query)
	ss := &Stmt{
		stmt: stmt{
			statement: buf,
			arguments: args,
		},
		replacer: c.dialect,
	}
	return ss
}

func (c Client) execStmt(s *stmt) error {
	ss := &Stmt{
		stmt:     *s,
		replacer: c.dialect,
	}
	ss.startTrace()
	defer func() {
		ss.stopTrace()
		c.consoleLog(ss)
	}()
	result, err := c.PrepareExec(ss.Raw(), ss.arguments...)
	if err != nil {
		return err
	}
	ss.Result = result
	return nil
}

func (c Client) execQuery(s *stmt) (*sql.Rows, error) {
	ss := &Stmt{
		stmt:     *s,
		replacer: c.dialect,
	}
	ss.startTrace()
	defer func() {
		ss.stopTrace()
		c.consoleLog(ss)
	}()
	var rows, err = c.Query(ss.Raw(), ss.arguments...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) execQueryRow(s *stmt) *sql.Row {
	ss := &Stmt{
		stmt:     *s,
		replacer: c.dialect,
	}
	ss.startTrace()
	defer func() {
		ss.stopTrace()
		c.consoleLog(ss)
	}()
	return c.QueryRow(ss.Raw(), ss.arguments...)
}

// PrepareExec :
func (c Client) PrepareExec(query string, args ...interface{}) (sql.Result, error) {
	conn, err := c.sqlCommon.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("goloquent: unable to prepare sql statement : %v", err)
	}
	defer conn.Close()
	result, err := conn.Exec(args...)
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}
	return result, nil
}

// Exec :
func (c Client) Exec(query string, args ...interface{}) (sql.Result, error) {
	result, err := c.sqlCommon.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}
	return result, nil
}

// Query :
func (c Client) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := c.sqlCommon.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}
	return rows, nil
}

// QueryRow :
func (c Client) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.sqlCommon.QueryRow(query, args...)
}

// DB :
type DB struct {
	id      string
	driver  string
	name    string
	replica string
	client  Client
	dialect Dialect
	omits   []string
}

// NewDB :
func NewDB(driver string, charset CharSet, conn sqlCommon, dialect Dialect, logHandler LogHandler) *DB {
	client := Client{
		driver:    driver,
		sqlCommon: conn,
		CharSet:   charset,
		dialect:   dialect,
		logger:    logHandler,
	}
	dialect.SetDB(client)
	return &DB{
		id:      fmt.Sprintf("%s:%d", driver, time.Now().UnixNano()),
		driver:  driver,
		name:    dialect.CurrentDB(),
		client:  client,
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
		client:  db.client,
		dialect: db.dialect,
	}
}

// ID :
func (db DB) ID() string {
	return db.id
}

// Name :
func (db DB) Name() string {
	return db.name
}

// NewQuery :
func (db *DB) NewQuery() *Query {
	return newQuery(db)
}

// Query :
func (db *DB) Query(stmt string, args ...interface{}) (*sql.Rows, error) {
	return db.client.Query(stmt, args...)
}

// Exec :
func (db *DB) Exec(stmt string, args ...interface{}) (sql.Result, error) {
	return db.client.Exec(stmt, args...)
}

// Table :
func (db *DB) Table(name string) *Table {
	return &Table{name, db}
}

// Migrate :
func (db *DB) Migrate(model ...interface{}) error {
	return newBuilder(db.NewQuery()).migrateMultiple(model)
}

// Omit :
func (db *DB) Omit(fields ...string) Replacer {
	ff := newDictionary(fields)
	clone := db.clone()
	ff.delete(keyFieldName)
	ff.delete(pkColumn)
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
	return newBuilder(db.NewQuery()).delete(model, true)
}

// Destroy :
func (db *DB) Destroy(model interface{}) error {
	return newBuilder(db.NewQuery()).delete(model, false)
}

// Truncate :
func (db *DB) Truncate(model ...interface{}) error {
	ns := make([]string, 0, len(model))
	for _, m := range model {
		var table string
		v := reflect.Indirect(reflect.ValueOf(m))
		switch v.Type().Kind() {
		case reflect.String:
			table = v.String()
		case reflect.Struct:
			table = v.Type().Name()
		default:
			return errors.New("goloquent: unsupported model")
		}

		table = strings.TrimSpace(table)
		if table == "" {
			return errors.New("goloquent: missing table name")
		}
		ns = append(ns, table)
	}
	return newBuilder(db.NewQuery()).truncate(ns...)
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

// Ancestor :
func (db *DB) Ancestor(ancestor *datastore.Key) *Query {
	return db.NewQuery().Ancestor(ancestor)
}

// AnyOfAncestor :
func (db *DB) AnyOfAncestor(ancestors ...*datastore.Key) *Query {
	return db.NewQuery().AnyOfAncestor(ancestors...)
}

// Where :
func (db *DB) Where(field string, operator string, value interface{}) *Query {
	return db.NewQuery().Where(field, operator, value)
}

// Where :
func (db *DB) MatchAgainst(fields []string, value []string) *Query {
	return db.NewQuery().MatchAgainst(fields, value)
}

// RunInTransaction :
func (db *DB) RunInTransaction(cb TransactionHandler) error {
	return newBuilder(db.NewQuery()).runInTransaction(cb)
}

// Close :
func (db *DB) Close() error {
	x, isOk := db.client.sqlCommon.(*sql.DB)
	if !isOk {
		return nil
	}
	return x.Close()
}
