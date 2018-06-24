package test

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	_ "github.com/go-sql-driver/mysql"
	"github.com/si3nloong/goloquent"
	"github.com/si3nloong/goloquent/db"
)

var (
	mysql   *goloquent.DB
	nameKey = datastore.NameKey("Name", "hIL0O7zfZP", nil)
	idKey   = datastore.IDKey("ID", int64(5116745034367558422), nil)
)

func TestMysqlConn(t *testing.T) {
	log.Println("CONNECT TO MYSQL " + strings.Repeat("-", 80))
	conn, err := db.Open("mysql", db.Config{
		Username: "root",
		Database: "goloquent",
		Logger: func(stmt *goloquent.Stmt) {
			log.Println(fmt.Sprintf("[%.3fms] %s", stmt.TimeElapse().Seconds()*1000, stmt.String()))
		},
	})
	if err != nil {
		panic(err)
	}
	mysql = conn
}

func TestMigration(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL MIGRATION")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.Migrate(new(User)); err != nil {
		log.Fatal(err)
	}
}

func TestCreate(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL SINGLE CREATE")
	log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := mysql.Create(u); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL MULTI CREATE")
	log.Println(strings.Repeat("-", 100))
	users := []*User{getFakeUser(), getFakeUser()}
	if err := mysql.Create(&users); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL SINGLE CREATE WITH PARENT KEY (NAME KEY)")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.Create(u, nameKey); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL SINGLE CREATE WITH PARENT KEY (ID KEY)")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.Create(u, idKey); err != nil {
		log.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	u := new(User)
	users := new([]User)
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL FIRST")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.First(u); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL FIND")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.Find(u.Key, u); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL GET")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.Get(users); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL GET WITH UNSCOPED")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.NewQuery().Unscoped().Get(users); err != nil {
		log.Fatal(err)
	}

}

func TestPaginate(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL PAGINATION")
	log.Println(strings.Repeat("-", 100))
	users := new([]User)
	p := &goloquent.Pagination{
		Limit: 10,
	}
	if err := mysql.Paginate(p, users); err != nil {
		log.Fatal(err)
	}
	log.Println("Records :", p.Count())
	log.Println("Cursor :", p.NextCursor())
}

func TestUpsert(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL SINGLE UPSERT")
	log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := mysql.Upsert(u); err != nil {
		log.Fatal(err)
	}

	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL MULTI UPSERT")
	log.Println(strings.Repeat("-", 100))
	users := []*User{getFakeUser(), getFakeUser()}
	if err := mysql.Upsert(&users); err != nil {
		log.Fatal(err)
	}
}

func TestSoftDelete(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL SOFT DELETE")
	log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := mysql.Create(u); err != nil {
		log.Fatal(err)
	}
	if err := mysql.Delete(u); err != nil {
		log.Fatal(err)
	}
}

func TestHardDelete(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL HARD DELETE")
	log.Println(strings.Repeat("-", 100))
	u := new(User)
	if err := mysql.First(u); err != nil {
		log.Fatal(err)
	}
	if err := mysql.Destroy(u); err != nil {
		log.Fatal(err)
	}
}

func TestRunInTransaction(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL RUN IN TRANSACTION")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.RunInTransaction(func(txn *goloquent.DB) error {
		u := new(User)
		if err := txn.NewQuery().
			WLock().First(u); err != nil {
			return err
		}

		u.Name = "NewName"
		u.UpdatedDateTime = time.Now().UTC()
		return txn.Save(u)
	}); err != nil {
		log.Fatal(err)
	}
}

func TestScan(t *testing.T) {
}

func TestTruncate(t *testing.T) {
	log.Println(strings.Repeat("-", 100))
	log.Println("MYSQL TRUNCATE")
	log.Println(strings.Repeat("-", 100))
	if err := mysql.Truncate(new(User)); err != nil {
		log.Fatal(err)
	}
}
