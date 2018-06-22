package test

import (
	"log"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/copier"
	"github.com/si3nloong/goloquent"
	"github.com/si3nloong/goloquent/db"
)

var mysql *goloquent.DB

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
	mysql = conn
}

func TestMigration(t *testing.T) {
	if err := mysql.Migrate(new(User)); err != nil {
		log.Fatal(err)
	}
}

func TestCreate(t *testing.T) {
	var u User
	copier.Copy(&u, &user)
	if err := mysql.Create(&u); err != nil {
		log.Fatal(err)
	}
}

func TestSoftDelete(t *testing.T) {
	var u User
	copier.Copy(&u, &user)
	if err := mysql.Create(&u); err != nil {
		log.Fatal(err)
	}
	if err := mysql.Delete(&u); err != nil {
		log.Fatal(err)
	}
}

func TestHardDelete(t *testing.T) {
	var u User
	copier.Copy(&u, &user)
	if err := mysql.Create(&u); err != nil {
		log.Fatal(err)
	}
	if err := mysql.Delete(&u); err != nil {
		log.Fatal(err)
	}
}
