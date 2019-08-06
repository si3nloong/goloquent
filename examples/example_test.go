package examples

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/si3nloong/goloquent"
	"github.com/si3nloong/goloquent/db"
	"github.com/si3nloong/goloquent/expr"

	//  "database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func TestExamples(t *testing.T) {

	conn, err := db.Open("mysql", db.Config{
		Username: "root",
		Password: "abcd1234",
		Host:     "localhost",
		Port:     "3306",
		Database: "goloquent",
		Logger: func(stmt *goloquent.Stmt) {
			log.Println(stmt.TimeElapse()) // elapse time in time.Duration
			log.Println(stmt.String())     // Sql string without any ?
			log.Println(stmt.Raw())        // Sql prepare statement
			log.Println(stmt.Arguments())  // Sql prepare statement's arguments
			log.Println(fmt.Sprintf("[%.3fms] %s", stmt.TimeElapse().Seconds()*1000, stmt.String()))
		},
	})
	// defer conn.Close()
	if err != nil {
		panic(err)
	}

	db.Migrate(new(User))
	db.Truncate("User")

	users := [...]User{
		newUser(),
		newUser(),
		newUser(),
		newUser(),
		newUser(),
	}
	db.Create(&users)
	usrs := []User{}
	db.NewQuery().OrderBy(
		expr.Field("Status", []string{
			"A", "B", "C",
		}),
		"CreatedAt",
	).Get(&usrs)

	log.Println(usrs)
	// db.Create()
	log.Println(conn)
}

func newUser() (u User) {
	u.Name = ""
	u.CreatedAt = time.Now()
	u.Status = gofakeit.RandString([]string{
		"A",
		"B",
		"C",
	})
	return
}
