package goloquent

import (
	"log"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestDatabase(t *testing.T) {
	driver := "mysql"
	dialect, _ := GetDialect(driver)
	conn, err := dialect.Open(Config{
		Username: "root",
		Password: "abcd1234",
		Database: "goloquent",
	})
	if err != nil {
		panic(err)
	}

	db := NewDB(driver, CharSet{}, conn, dialect, nil)
	log.Println(db)

	if err := db.Create(User{}); err != nil {
		log.Println(err)
		// t.FailNow()
	}

}
