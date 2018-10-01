package db

import (
	"fmt"
	"strings"
	"sync"

	"github.com/zypeh/goloquent"
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
	dialect, matched := goloquent.MatchStorageOperator(driver)
	if !matched {
		panic("goloquent: unsupported storage operator")
	}
	pool := make(map[string]*goloquent.DB)
	if p, isOk := connPool.Load(driver); isOk {
		pool = p.(map[string]*goloquent.DB)
	}
	config := goloquent.Config{
		Username:   conf.Username,
		Password:   conf.Password,
		Host:       conf.Host,
		Port:       conf.Port,
		Database:   conf.Database,
		UnixSocket: conf.UnixSocket,
		CharSet:    conf.CharSet,
		Logger:     conf.Logger,
	}
	config.Normalize()
	conn, err := dialect.Open(config)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("goloquent: %s server has not response", driver)
	}
	db := goloquent.NewDB(driver, *config.CharSet, conn, dialect, conf.Logger)
	pool[conf.Database] = db
	connPool.Store(driver, pool)
	// Override defaultDB wheneve initialise a new connection
	defaultDB = db
	return db, nil
}
