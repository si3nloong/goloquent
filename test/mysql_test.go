package test

import (
	"log"
	"strings"
	"testing"

	"github.com/si3nloong/goloquent"
	"github.com/si3nloong/goloquent/db"
)

var mysqlConn *goloquent.DB

func TestMysqlConn(t *testing.T) {
	conn, err := db.Open("mysql", db.Config{
		Username: "root",
		Database: "goloquent",
		Logger: func(stmt *goloquent.Stmt) {
			log.Println(stmt.String())
		},
	})
	if err != nil {
		panic(err)
	}
	mysqlConn = conn
}

func TestMigration(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("DEBUG MYSQL MIGRATION ")
	log.Println(strings.Repeat("-", 100))
	// if err := mysqlConn.Migrate(&i); err != nil {
	// 	fmt.Println("Error ::", err)
	// }
}

func TestCreate(t *testing.T) {

}
