package test

import (
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/si3nloong/goloquent"
	"github.com/si3nloong/goloquent/db"
)

var (
	pg *goloquent.DB
)

func TestPostgresConn(t *testing.T) {
	// log.Println("CONNECT TO POSTGRES " + strings.Repeat("-", 80))
	conn, err := db.Open("postgres", db.Config{
		Username: "sianloong",
		Database: "goloquent",
		Logger: func(stmt *goloquent.Stmt) {
			log.Println(fmt.Sprintf("[%.3fms] %s", stmt.TimeElapse().Seconds()*1000, stmt.String()))
		},
	})
	if err != nil {
		panic(err)
	}
	pg = conn
}

func TestPostgresMigration(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES MIGRATION")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.Migrate(new(User)); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresTableExists(t *testing.T) {
	if isExist := pg.Table("User").Exists(); isExist != true {
		log.Fatal(fmt.Errorf("Unexpected error, table %q should exists", "User"))
	}
}

func TestPostgresAddIndex(t *testing.T) {
	if err := pg.Table("User").
		AddUniqueIndex("Username"); err != nil {
		log.Fatal(err)
	}
	if err := pg.Table("User").
		AddIndex("Age"); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresCreate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES SINGLE CREATE")
	// log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := pg.Create(u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES SINGLE CREATE WITH PARENT KEY (NAME KEY)")
	// log.Println(strings.Repeat("-", 100))
	u = getFakeUser()
	if err := pg.Create(u, nameKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES SINGLE CREATE WITH PARENT KEY (ID KEY)")
	// log.Println(strings.Repeat("-", 100))
	u = getFakeUser()
	if err := pg.Create(u, idKey); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES MULTI CREATE")
	// log.Println(strings.Repeat("-", 100))

	users := []*User{getFakeUser(), getFakeUser()}
	if err := pg.Create(&users); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresSelect(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES FIRST WITH SELECT QUERY")
	// log.Println(strings.Repeat("-", 100))
	u := new(User)
	if err := pg.
		Select("*", "Name").First(u); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresDistinctOn(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES DISTINCT ON WITH *")
	// log.Println(strings.Repeat("-", 100))
	u := new(User)
	if err := pg.NewQuery().
		DistinctOn("*").First(u); err == nil {
		log.Fatal("Expected `DistinctOn` cannot allow *")
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES DISTINCT ON WITH EMPTY INPUT")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.NewQuery().
		DistinctOn("").First(u); err == nil {
		log.Fatal("Expected `DistinctOn` cannot have empty")
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES DISTINCT ON WITH COLUMN")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.NewQuery().
		DistinctOn("Name", "Password").First(u); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresGet(t *testing.T) {
	u := new(User)
	users := new([]User)
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES FIRST")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.First(u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES FIND")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.Find(u.Key, u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES GET")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.Get(users); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES GET WITH UNSCOPED")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.NewQuery().Unscoped().Get(users); err != nil {
		log.Fatal(err)
	}

}

func TestPostgresJSON(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONEqual("Address:PostCode", 85).
		Get(users); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresPaginate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES PAGINATION")
	// log.Println(strings.Repeat("-", 100))
	users := new([]User)
	p := &goloquent.Pagination{
		Limit: 10,
	}
	if err := pg.Paginate(p, users); err != nil {
		log.Fatal(err)
	}
	// log.Println("Records :", p.Count())
	// log.Println("Cursor :", p.NextCursor())
}

func TestPostgresUpsert(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES SINGLE UPSERT")
	// log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := pg.Upsert(u); err != nil {
		log.Fatal(err)
	}

	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES MULTI UPSERT")
	// log.Println(strings.Repeat("-", 100))
	users := []*User{getFakeUser(), getFakeUser()}
	if err := pg.Upsert(&users); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresUpdate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES UPDATE")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.Table("User").Limit(1).
		Where("Name", "=", "Dr. Antoinette Zboncak").
		Update(map[string]interface{}{
			"Name": "sianloong",
		}); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresSoftDelete(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES SOFT DELETE")
	// log.Println(strings.Repeat("-", 100))
	u := getFakeUser()
	if err := pg.Create(u); err != nil {
		log.Fatal(err)
	}
	if err := pg.Delete(u); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresHardDelete(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES HARD DELETE")
	// log.Println(strings.Repeat("-", 100))
	u := new(User)
	if err := pg.First(u); err != nil {
		log.Fatal(err)
	}
	if err := pg.Destroy(u); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresTable(t *testing.T) {
	users := new([]User)
	if err := pg.Table("User").
		WhereLike("Name", "nick%").
		Get(users); err != nil {
		log.Fatal(err)
	}

	if err := pg.Table("User").
		Where("Age", ">", 0).
		Get(users); err != nil {
		log.Fatal(err)
	}

	user := new(User)
	if err := pg.Table("User").
		First(user); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresRunInTransaction(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES RUN IN TRANSACTION")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.RunInTransaction(func(txn *goloquent.DB) error {
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

func TestPostgresScan(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES SCAN")
	// log.Println(strings.Repeat("-", 100))
	var count, sum uint
	if err := pg.Table("User").
		Select("COALESCE(COUNT(*),0)", fmt.Sprintf("COALESCE(SUM(%q),0)", "Age")).
		Scan(&count, &sum); err != nil {
		log.Fatal(err)
	}
	// log.Println("Count :", count, ", Sum :", sum)
}

func TestPostgresTruncate(t *testing.T) {
	// log.Println(strings.Repeat("-", 100))
	// log.Println("POSTGRES TRUNCATE")
	// log.Println(strings.Repeat("-", 100))
	if err := pg.Truncate(new(User)); err != nil {
		log.Fatal(err)
	}
}

func TestPostgresDropTableIfExists(t *testing.T) {
	// if err := pg.Table("User").DropIfExists(); err != nil {
	// 	log.Fatal(err)
	// }
}
