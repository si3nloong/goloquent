package test

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	_ "github.com/go-sql-driver/mysql"
	"github.com/si3nloong/goloquent"
	"github.com/si3nloong/goloquent/db"
)

var (
	my      *goloquent.DB
	nameKey = datastore.NameKey("Name", "hIL0O7zfZP", nil)
	idKey   = datastore.IDKey("ID", int64(5116745034367558422), nil)
)

func TestMySQLConn(t *testing.T) {
	// log.Println("CONNECT TO MYSQL " + strings.Repeat("-", 80))
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
	my = conn
}

func TestMySQLMigration(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MIGRATION")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Migrate(new(User)); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLTableExists(t *testing.T) {
	if isExist := my.Table("User").Exists(); isExist != true {
		log.Fatal(fmt.Errorf("Unexpected error, table %q should exists", "User"))
	}
}

func TestMySQLAddIndex(t *testing.T) {
	if err := my.Table("User").
		AddUniqueIndex("Username"); err != nil {
		log.Fatal(err)
	}
	if err := my.Table("User").
		AddIndex("Age"); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLCreate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SINGLE CREATE")
	// log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := my.Create(u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SINGLE CREATE WITH PARENT KEY (NAME KEY)")
	// log.Println(strings.Repeat("-", 100))
	u = getFakeUser()
	if err := my.Create(u, nameKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SINGLE CREATE WITH PARENT KEY (ID KEY)")
	// log.Println(strings.Repeat("-", 100))
	u = getFakeUser()
	if err := my.Create(u, idKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MULTI CREATE WITH SLICE STRUCT")
	// log.Println(strings.Repeat("-", 100))
	uu := []User{*getFakeUser(), *getFakeUser()}
	if err := my.Create(&uu); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MULTI CREATE WITH SLICE POINTER STRUCT")
	// log.Println(strings.Repeat("-", 100))
	users := []*User{getFakeUser(), getFakeUser()}
	if err := my.Create(&users); err != nil {
		log.Fatal(err)
	}

}

func TestJSONMarshal(t *testing.T) {
	u := new(User)
	if err := my.Select("*", "Name").First(u); err != nil {
		log.Fatal(err)
	}

	b, err := json.Marshal(u)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(b))
}

func TestMySQLSelect(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL FIRST WITH SELECT QUERY")
	// log.Println(strings.Repeat("-", 100))
	u := new(User)
	if err := my.Select("*", "Name").First(u); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLDistinctOn(t *testing.T) {
	u := new(User)
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL DISTINCT ON WITH *")
	// log.Println(strings.Repeat("-", 100))
	if err := my.NewQuery().
		DistinctOn("*").First(u); err == nil {
		log.Fatal("Expected `DistinctOn` cannot allow *")
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL DISTINCT ON WITH EMPTY INPUT")
	// log.Println(strings.Repeat("-", 100))
	if err := my.NewQuery().
		DistinctOn("").First(u); err == nil {
		log.Fatal("Expected `DistinctOn` cannot have empty")
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL DISTINCT ON WITH COLUMN")
	// log.Println(strings.Repeat("-", 100))
	if err := my.NewQuery().
		DistinctOn("Name", "Password").First(u); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLEmptySliceInJSON(t *testing.T) {
	u := new(User)
	if err := my.First(u); err != nil {
		log.Fatal(err)
	}
	if u.Email == nil {
		log.Fatal(fmt.Errorf("empty slice should init on any `Get` func"))
	}

	u2 := getFakeUser()
	u2.Email = nil
	if err := my.Create(u2); err != nil {
		log.Fatal(err)
	}
	if u2.Email == nil {
		log.Fatal(fmt.Errorf("empty slice should init on any `Create` func"))
	}
}

func TestMySQLGet(t *testing.T) {
	u := new(User)
	users := new([]User)
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL FIRST")
	// log.Println(strings.Repeat("-", 100))
	if err := my.First(u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL FIND")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Find(u.Key, u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL GET")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Get(users); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL GET WITH UNSCOPED")
	// log.Println(strings.Repeat("-", 100))
	if err := my.NewQuery().Unscoped().Get(users); err != nil {
		log.Fatal(err)
	}

}

func TestMySQLJSONEqual(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONEqual("Address > PostCode", 85).
		Get(users); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLJSONIn(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONEqual("Address > PostCode", 85).
		Get(users); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLPaginate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL PAGINATION")
	// log.Println(strings.Repeat("-", 100))
	users := new([]User)
	p := &goloquent.Pagination{
		Limit: 10,
	}
	if err := my.Paginate(p, users); err != nil {
		log.Fatal(err)
	}
	// log.Println("Records :", p.Count())
	// log.Println("Cursor :", p.NextCursor())
}

func TestMySQLUpsert(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SINGLE UPSERT")
	// log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := my.Upsert(u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SINGLE UPSERT WITH PARENT KEY (ID KEY)")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Upsert(u, idKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SINGLE UPSERT WITH PARENT KEY (NAME KEY)")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Upsert(u, nameKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MULTI UPSERT WITH SLICE POINTER STRUCT")
	// log.Println(strings.Repeat("-", 100))
	users := []*User{getFakeUser(), getFakeUser()}
	if err := my.Upsert(&users); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MULTI UPSERT WITH SLICE STRUCT")
	// log.Println(strings.Repeat("-", 100))
	uu := []User{*getFakeUser(), *getFakeUser()}
	if err := my.Upsert(&uu); err != nil {
		log.Fatal(err)
	}

	uuu := []User{*getFakeUser(), *getFakeUser()}
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MULTI UPSERT WITH SLICE STRUCT AND PARENT KEY (ID KEY)")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Upsert(&uuu, idKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL MULTI UPSERT WITH SLICE STRUCT AND PARENT KEY (NAME KEY)")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Upsert(&uuu, nameKey); err != nil {
		log.Fatal(err)
	}

}

func TestMySQLUpdate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL UPDATE")
	// log.Println(strings.Repeat("-", 100))
	if err := my.Table("User").Limit(1).
		Where("Name", "=", "Dr. Antoinette Zboncak").
		Update(map[string]interface{}{
			"Name": "sianloong",
		}); err != nil {
		log.Fatal(err)
	}

	if err := my.Table("User").Limit(1).
		Update(map[string]interface{}{
			"Email": []string{"abc@gmail.com", "abc@hotmail.com", "abc@yahoo.com"},
		}); err != nil {
		log.Fatal(err)
	}

	// TODO: support struct
	// if err := my.Table("User").Limit(1).
	// 	Update(map[string]interface{}{
	// 		"Address": Address{"", "Line2", "", 63000},
	// 	}); err != nil {
	// 	log.Fatal(err)
	// }
}
func TestMySQLSoftDelete(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SOFT DELETE")
	// log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := my.Create(u); err != nil {
		log.Fatal(err)
	}
	if err := my.Delete(u); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLHardDelete(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL HARD DELETE")
	// log.Println(strings.Repeat("-", 100))
	u := new(User)
	if err := my.First(u); err != nil {
		log.Fatal(err)
	}
	if err := my.Destroy(u); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLTable(t *testing.T) {
	users := new([]User)
	if err := my.Table("User").
		WhereLike("Name", "nick%").
		Get(users); err != nil {
		log.Fatal(err)
	}

	if err := my.Table("User").
		Where("Age", ">", 0).
		Get(users); err != nil {
		log.Fatal(err)
	}

	user := new(User)
	if err := my.Table("User").
		First(user); err != nil {
		log.Fatal(err)
	}
}

func TestMySQLRunInTransaction(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL RUN IN TRANSACTION")
	// log.Println(strings.Repeat("-", 100))
	if err := my.RunInTransaction(func(txn *goloquent.DB) error {
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

func TestMySQLScan(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL SCAN")
	// log.Println(strings.Repeat("-", 100))
	var count, sum uint
	if err := my.Table("User").Select("COALESCE(COUNT(*),0), COALESCE(SUM(Age),0)").
		Scan(&count, &sum); err != nil {
		log.Fatal(err)
	}
	log.Println("Count :", count, ", Sum :", sum)
}

func TestMySQLTruncate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("MYSQL TRUNCATE")
	// log.Println(strings.Repeat("-", 100))
	// if err := my.Truncate(new(User)); err != nil {
	// 	log.Fatal(err)
	// }
}

func TestMySQLDropTableIfExists(t *testing.T) {
	// if err := my.Table("User").DropIfExists(); err != nil {
	// 	log.Fatal(err)
	// }
}
