package test

import (
	"encoding/json"
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

func TestPostgresDropTableIfExists(t *testing.T) {
	if err := pg.Table("User").DropIfExists(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMigration(t *testing.T) {
	if err := pg.Migrate(new(User), new(TempUser)); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTableExists(t *testing.T) {
	if isExist := pg.Table("User").Exists(); isExist != true {
		t.Fatal(fmt.Errorf("Unexpected error, table %q should exists", "User"))
	}
}

func TestPostgresTruncate(t *testing.T) {
	if err := pg.Truncate(new(User), TempUser{}); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAddIndex(t *testing.T) {
	if err := pg.Table("User").
		AddUniqueIndex("Username"); err != nil {
		t.Fatal(err)
	}
	if err := pg.Table("User").
		AddIndex("Age"); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresEmptyInsertOrUpsert(t *testing.T) {
	var users []User
	if err := pg.Create(&users); err != nil {
		t.Fatal(err)
	}

	if err := pg.Upsert(&users); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCreate(t *testing.T) {
	u := getFakeUser()
	if err := pg.Create(u); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := pg.Create(u, nameKey); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := pg.Create(u, idKey); err != nil {
		t.Fatal(err)
	}

	users := []*User{getFakeUser(), getFakeUser()}
	if err := pg.Create(&users); err != nil {
		t.Fatal(err)
	}
}

// func TestPostgresReplaceInto(t *testing.T) {
// 	if err := pg.Table("User").
// 		AnyOfAncestor(nameKey, idKey).
// 		ReplaceInto("TempUser"); err != nil {
// 		t.Fatal(err)
// 	}
// }

func TestPostgresSelect(t *testing.T) {
	u := new(User)
	if err := pg.
		Select("*", "Name").First(u); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresDistinctOn(t *testing.T) {
	u := new(User)
	if err := pg.NewQuery().
		DistinctOn("*").First(u); err == nil {
		t.Fatal("Expected `DistinctOn` cannot allow *")
	}

	if err := pg.NewQuery().
		DistinctOn("").First(u); err == nil {
		t.Fatal("Expected `DistinctOn` cannot have empty")
	}

	if err := pg.NewQuery().
		DistinctOn("Name", "Password").First(u); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresGet(t *testing.T) {
	u := new(User)
	users := new([]User)
	if err := pg.First(u); err != nil {
		t.Fatal(err)
	}

	if err := pg.Find(u.Key, u); err != nil {
		t.Fatal(err)
	}

	if err := pg.Get(users); err != nil {
		t.Fatal(err)
	}

	if err := pg.NewQuery().Unscoped().Get(users); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWhereFilter(t *testing.T) {
	age := uint8(85)
	creditLimit := float64(100.015)
	dob, _ := time.Parse("2006-01-02", "1900-10-01")

	u := getFakeUser()
	u.Age = age
	u.Nickname = nil
	u.CreditLimit = creditLimit
	u.Birthdate = goloquent.Date(dob)

	pg.Create(u)

	users := new([]User)
	if err := pg.Where("Age", "=", &age).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}

	if err := pg.Where("Birthdate", "=", goloquent.Date(dob)).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}

	var nilNickname *string
	if err := pg.Where("Nickname", "=", nilNickname).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}

	if err := pg.Where("CreditLimit", "=", &creditLimit).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}
}

func TestPostgresWhereAnyLike(t *testing.T) {
	users := new([]User)

	u := getFakeUser()
	u.PrimaryEmail = "sianloong@hotmail.com"
	if err := pg.Create(u); err != nil {
		t.Fatal(err)
	}

	if err := pg.NewQuery().
		WhereAnyLike("PrimaryEmail", []string{
			"lzPskFb@OOxzA.net",
			"sianloong%",
		}).Get(users); err != nil {
		t.Fatal(err)
	}

	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "WhereAnyLike"`)
	}
}
func TestPostgresJSONRawMessage(t *testing.T) {
	u := getFakeUser()
	if err := pg.Upsert(u); err != nil {
		t.Fatal(err)
	}
	u.Information = nil
	if err := pg.Upsert(u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`[]`)
	if err := pg.Upsert(u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`{}`)
	if err := pg.Upsert(u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`null`)
	if err := pg.Upsert(u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`notvalid`)
	if err := pg.Upsert(u); err == nil {
		t.Fatal(err)
	}
}

func TestPostgresEmptySliceInJSON(t *testing.T) {
	u := new(User)
	if err := pg.First(u); err != nil {
		t.Fatal(err)
	}
	if u.Emails == nil {
		t.Fatal(fmt.Errorf("empty slice should init on any `Get` func"))
	}

	u2 := getFakeUser()
	u2.Emails = nil
	u2.PrimaryEmail = "sianloong@hotmail.com"
	if err := pg.Create(u2); err != nil {
		t.Fatal(err)
	}
	if u2.Emails == nil {
		t.Fatal(fmt.Errorf("empty slice should init on any `Create` func"))
	}
}

func TestPostgresJSONEqual(t *testing.T) {
	var emptyStr string

	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONEqual("Address>PostCode", int32(85)).
		Get(users); err != nil {
		t.Fatal(err)
	}

	if err := pg.NewQuery().
		WhereJSONEqual("Address>PostCode", uint32(85)).
		Get(users); err != nil {
		t.Fatal(err)
	}

	postCode := uint32(85)
	if err := pg.NewQuery().
		WhereJSONEqual("Address>PostCode", &postCode).
		Get(users); err != nil {
		t.Fatal(err)
	}

	if err := pg.NewQuery().
		WhereJSONEqual("Address>Line1", "7812, Jalan Section 22").
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}

	if err := pg.NewQuery().
		WhereJSONEqual("Address>Line2", emptyStr).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}

	timeZone := new(time.Time)
	if err := pg.NewQuery().
		WhereJSONEqual("Address>region.TimeZone", timeZone).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}
}

func TestPostgresJSONNotEqual(t *testing.T) {
	var timeZone *time.Time
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONNotEqual("Address>region.TimeZone", timeZone).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}

	if err := pg.NewQuery().
		WhereJSONNotEqual("Address>Country", "").
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) > 0 {
		t.Fatal("JSON equal has unexpected result")
	}
}

func TestPostgresJSONIn(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONIn("Address>PostCode", []interface{}{0, 10, 20}).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON contain any has unexpected result")
	}
}

func TestPostgresJSONNotIn(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONNotIn("Address>Line1", []interface{}{"PJ", "KL", "Cheras"}).
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON contain any has unexpected result")
	}
}

func TestPostgresJSONContainAny(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONContainAny("Emails", []Email{
			"support@hotmail.com",
			"invalid@gmail.com",
		}).Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON contain any has unexpected result")
	}

	if err := pg.NewQuery().
		WhereJSONContainAny("Emails", []Email{
			"invalid@gmail.com",
			"invalid@hotmail.com",
		}).Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) > 0 {
		t.Fatal("JSON contain any has unexpected result")
	}
}

func TestPostgresJSONType(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONType("Address>region", "OBJECT").
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON isObject has unexpected result")
	}
}

func TestPostgresJSONIsObject(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONIsObject("Address>region").
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON isObject has unexpected result")
	}
}

func TestPostgresJSONIsArray(t *testing.T) {
	users := new([]User)
	if err := pg.NewQuery().
		WhereJSONIsArray("Address>region.keys").
		Get(users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON isArray has unexpected result")
	}
}

func TestPostgresPaginate(t *testing.T) {
	users := new([]User)

	uu := []*User{getFakeUser(), getFakeUser(), getFakeUser()}
	if err := pg.Create(&uu, nameKey); err != nil {
		t.Fatal(err)
	}
	p := &goloquent.Pagination{
		Limit: 1,
	}
	if err := pg.Ancestor(nameKey).
		Paginate(p, users); err != nil {
		t.Fatal(err)
	}
	if len(*(users)) <= 0 {
		t.Fatal(fmt.Errorf("paginate record set shouldn't empty"))
	}

	// p.Cursor = p.NextCursor()
	// if err := pg.Ancestor(nameKey).
	// 	Paginate(p, users); err != nil {
	// 	t.Fatal(err)
	// }
	// if len(*(users)) <= 0 {
	// 	t.Fatal(fmt.Errorf("paginate record set shouldn't empty"))
	// }
}

func TestPostgresUpsert(t *testing.T) {
	u := getFakeUser()
	if err := pg.Upsert(u); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := pg.Upsert(u, idKey); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := pg.Upsert(u, nameKey); err != nil {
		t.Fatal(err)
	}

	users := []*User{getFakeUser(), getFakeUser()}
	if err := pg.Upsert(&users); err != nil {
		t.Fatal(err)
	}

	uu := []User{*getFakeUser(), *getFakeUser()}
	if err := pg.Upsert(&uu); err != nil {
		t.Fatal(err)
	}

	uuu := []User{*getFakeUser(), *getFakeUser()}
	if err := pg.Upsert(&uuu, idKey); err != nil {
		t.Fatal(err)
	}

	uuu = []User{*getFakeUser(), *getFakeUser()}
	if err := pg.Upsert(&uuu, nameKey); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresUpdate(t *testing.T) {
	if err := pg.Table("User").Limit(1).
		Where("Name", "=", "Dr. Antoinette Zboncak").
		Update(map[string]interface{}{
			"Name": "sianloong",
		}); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSoftDelete(t *testing.T) {
	u := getFakeUser()
	if err := pg.Create(u); err != nil {
		t.Fatal(err)
	}
	if err := pg.Delete(u); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresHardDelete(t *testing.T) {
	u := new(User)
	if err := pg.First(u); err != nil {
		t.Fatal(err)
	}
	if err := pg.Destroy(u); err != nil {
		t.Fatal(err)
	}
}

// func TestPostgresTable(t *testing.T) {
// 	users := new([]User)
// 	if err := pg.Table("User").
// 		WhereLike("Name", "nick%").
// 		Get(users); err != nil {
// 		t.Fatal(err)
// 	}

// 	if err := pg.Table("User").
// 		Where("Age", ">", 0).
// 		Get(users); err != nil {
// 		t.Fatal(err)
// 	}

// 	user := new(User)
// 	if err := pg.Table("User").
// 		First(user); err != nil {
// 		t.Fatal(err)
// 	}
// }

func TestPostgresRunInTransaction(t *testing.T) {
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
		t.Fatal(err)
	}
}

func TestPostgresScan(t *testing.T) {
	var count, sum uint
	if err := pg.Table("User").
		Select("COALESCE(COUNT(*),0)", `COALESCE(SUM("Age"),0)`).
		Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	log.Println("Count :", count, ", Sum :", sum)
}

func TestPostgresClose(t *testing.T) {
	defer pg.Close()
}
