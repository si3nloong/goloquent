package db

import (
	"fmt"
	"strings"
	"sync"

	"github.com/si3nloong/goloquent"
)

var (
	defaultDB *goloquent.DB
	connPool  sync.Map // database connection pools
)

// Config :
type Config struct {
	Username   string
	Password   string
	Host       string
	Port       string
	Database   string
	UnixSocket string
	CharSet    *goloquent.CharSet
	Logger     goloquent.LogHandler
}

// Open :
func Open(driver string, conf Config) (*goloquent.DB, error) {
	driver = strings.TrimSpace(strings.ToLower(driver))
	dialect, isValid := goloquent.GetDialect(driver)
	if !isValid {
		panic(fmt.Errorf("goloquent: unsupported database driver %q", driver))
	}
	pool := make(map[string]*goloquent.DB)
	if p, isOk := connPool.Load(driver); isOk {
		pool = p.(map[string]*goloquent.DB)
	}
	conn, err := dialect.Open(goloquent.Config{
		Username:   conf.Username,
		Password:   conf.Password,
		Host:       conf.Host,
		Port:       conf.Port,
		Database:   conf.Database,
		UnixSocket: conf.UnixSocket,
		CharSet:    conf.CharSet,
		Logger:     conf.Logger,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("goloquent: mysql server has not response")
	}
	db := goloquent.NewDB(driver, conn, dialect, conf.Logger)
	pool[conf.Database] = db
	connPool.Store(driver, pool)
	// Override defaultDB wheneve initialise a new connection
	defaultDB = db
	return db, nil
}
