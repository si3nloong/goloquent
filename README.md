
This package is not compactible with native package `database/sql`, if you want the support of it, you may go for [sqlike](https://github.com/si3nloong/sqlike)

# Sequel Datastore ORM

Inspired by Laravel Eloquent and Google Cloud Datastore

This repo still under development. We accept any pull request. ^\_^

## Database Support

- [x] MySQL (version 5.7 and above)
- [x] Postgres (version 9.4 and above)

## Installation

```bash
  // dependency
  $ go get -u github.com/go-sql-driver/mysql // Mysql
  $ go get -u github.com/lib/pq // Postgres
  $ go get -u cloud.google.com/go/datastore
  $ go get -u github.com/si3nloong/goloquent
```

- **Import the library**

```go
  import "github.com/si3nloong/goloquent"
```

## Quick Start

### Connect to database

```go
    import "github.com/si3nloong/goloquent/db"

    conn, err := db.Open("mysql", db.Config{
        Username: "root",
        Password: "",
        Host: "localhost",
        Port: "3306",
        Database: "test",
        Logger: func(stmt *goloquent.Stmt) {
            log.Println(stmt.TimeElapse()) // elapse time in time.Duration
            log.Println(stmt.String()) // Sql string without any ?
            log.Println(stmt.Raw()) // Sql prepare statement
            log.Println(stmt.Arguments()) // Sql prepare statement's arguments
            log.Println(fmt.Sprintf("[%.3fms] %s", stmt.TimeElapse().Seconds()*1000, stmt.String()))
        },
    })
    defer conn.Close()
    if err != nil {
        panic("Connection error: ", err)
    }
```

#### User Table

```go
// Address :
type Address struct {
	Line1    string
	Line2    string
	Country  string
	PostCode uint
	Region   struct {
		TimeZone    time.Time
		Keys        []*datastore.Key   `goloquent:"keys"`
		CountryCode string             `goloquent:"regionCode"`
		Geolocation datastore.GeoPoint `goloquent:"geo"`
	} `goloquent:"region"`
}

// User : User kind parent is Merchant
type User struct {
    Key             *datastore.Key `goloquent:"__key__"` // load table key
    Name            string
    Nicknames       []string
    CountryCode     string
    PhoneNumber     string
    Email           string
    BirthDate       goloquent.Date
    Address         Address
    Age             uint8
    ExtraInfo       json.RawMessage
    CreatedDateTime time.Time
    UpdatedDateTime time.Time
}

// Load : load func will execute after load
func (x *User) Load() error {
	return nil
}

// Save : save func will execute before save
func (x *User) Save() (error) {
	return nil
}
```

### Helper

```go
    // StringPrimaryKey is to get key value in string form
    key := datastore.NameKey("Merchant", "mjfFgYnxBS", nil)
    fmt.Println(goloquent.StringKey(key)) // "mjfFgYnxBS"

    key = datastore.IDKey("User", int64(2305297334603281546), nil)
    fmt.Println(goloquent.StringKey(key)) // "2305297334603281546"

    key = datastore.NameKey("User", int64(2305297334603281546), datastore.NameKey("Merchant", "mjfFgYnxBS", nil))
    fmt.Println(goloquent.StringifyKey(key)) // Merchant,'mjfFgYnxBS'/User,2305297334603281546
```

### Table

```go
    import "github.com/si3nloong/goloquent/db"

    // Check table exists
    db.Table("User").Exists() // true

    // Drop table if exists
    if err := db.Table("User").DropIfExists(); err != nil {
        log.Fatal(err)
    }

    // Add index
    if err := db.Table("User").AddIndex("Name", "Email"); err != nil {
        log.Fatal(err)
    }

    // Add unique index
    if err := db.Table("User").AddUniqueIndex("Email"); err != nil {
        log.Fatal(err)
    }
```

### Create Record

```go
    import "github.com/si3nloong/goloquent/db"

    // Example
    user := new(User)
    user.Name = "Hello World"
    user.Age = 18

    // OR
    var user *User
    user.Name = "Hello World"
    user.Age = 18

    // Create without parent key
    if err := db.Create(user, nil); err != nil {
        log.Println(err) // fail to create record
    }

    // Create with parent key
    parentKey := datastore.NameKey("Parent", "value", nil)
    if err := db.Create(user, parentKey); err != nil {
        log.Println(err) // fail to create record
    }

    // Create without key, goloquent will auto generate primary key
    if err := db.Create(user); err != nil {
        log.Println(err) // fail to create record
    }

    // Create with self generate key
    user.Key = datastore.NameKey("User", "uniqueID", nil)
    if err := db.Create(user); err != nil {
        log.Println(err) // fail to create record
    }
```

### Upsert Record

```go
    import "github.com/si3nloong/goloquent/db"

    // Example
    user := new(User)
    user.Name = "Hello World"
    user.Age = 18

    // Update if key exists, else create the user record
    parentKey := datastore.NameKey("Parent", "value", nil)
    if err := db.Upsert(user, parentKey); err != nil {
        log.Println(err) // fail
    }

    // Upsert with self generate key
    user := new(User)
    user.Key = datastore.NameKey("User", "uniqueID", nil)
    user.Name = "Hello World"
    user.Age = 18
    if err := db.Upsert(user); err != nil {
        log.Println(err) // fail
    }
```

### Retrieve Record

- **Get Single Record using Primary Key**

```go
    // Example
    primaryKey := datastore.IDKey("User", int64(2305297334603281546), nil)
    user := new(User)
    if err := db.Find(primaryKey, user); err != nil {
        log.Println(err) // error while retrieving record
    }

    if err := db.Where("Status", "=", "ACTIVE").
        Find(primaryKey, user); err != goloquent.ErrNoSuchEntity {
        // if no record found using primary key, error `ErrNoSuchEntity` will throw instead
        log.Println(err) // error while retrieving record
    }
```

- **Get Single Record**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example 1
    user := new(User)
    if err := db.First(user); err != nil {
        log.Println(err) // error while retrieving record
    }

    if user.Key != nil { // if have record
        fmt.Println("Have record")
    } else { // no record
        fmt.Println("Doesnt't have record")
    }

    // Example 2
    user := new(User)
    if err := db.Where("Email", "=", "admin@hotmail.com").
        First(user); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Example 3
    age := 22
    parentKey := datastore.IDKey("Parent", 1093, nil)
    user := new(User)
    if err := db.Ancestor(parentKey).
        WhereEqual("Age", &age).
        OrderBy("-CreatedDateTime").
        First(user); err != nil {
        log.Println(err) // error while retrieving record
    }
```

- **Get Multiple Record**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example 1
    users := new([]User)
    if err := db.Limit(10).Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Example 2
    users := new([]*User)
    if err := db.WhereEqual("Name", "Hello World").
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Example 3
    users := new([]User)
    if err := db.Ancestor(parentKey).
        WhereEqual("Name", "myz").
        WhereEqual("Age", 22).
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }
```

- **Get Record with OrderBying**

```go
    import "github.com/si3nloong/goloquent/db"
    // Ascending OrderBy
    users := new([]*User)
    if err := db.OrderBy("CreatedDateTime").
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Descending OrderBy
    if err := db.Table("User").
        OrderBy("-CreatedDateTime").
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }
```

- **Pagination Record**

```go
    import "github.com/si3nloong/goloquent/db"

    p := goloquent.Pagination{
        Limit:  10,
        Cursor: "", // pass the cursor that generate by the query so that it will display the next record
    }

    // Example
    users := new([]*User)
    if err := db.Ancestor(parentKey).
        OrderBy("-CreatedDateTime").
        Paginate(&p, users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // ***************** OR ********************
    p := &goloquent.Pagination{
        Limit:  10, // number of records in each page
        Cursor: "EhQKCE1lcmNoYW50EK3bueKni5eNIxIWCgxMb3lhbHR5UG9pbnQaBkZrUUc4eA", // pass the cursor to get next record set
    }

    users := new([]*User)
    if err := db.Ancestor(parentKey).
        OrderBy("-CreatedDateTime").
        Paginate(p, users); err != nil {
        log.Println(err) // error while retrieving record
    }

    log.Println(p.NextCursor()) // next page cursor
    log.Println(p.Count()) // record count
```

### Save Record

```go
    import "github.com/si3nloong/goloquent/db"
    // Example
    if err := db.Save(user); err != nil {
        log.Println(err) // fail to delete record
    }
```

### Delete Record

- **Delete using Primary Key**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example
    if err := db.Delete(user); err != nil {
        log.Println(err) // fail to delete record
    }
```

- **Delete using Where statement**

```go
    // Delete user table record which account type not equal to "PREMIUM" or "MONTLY"
    if err := db.Table("User").
        WhereNotIn("AccountType", []string{
            "PREMIUM", "MONTLY",
        }).Flush(); err != nil {
        log.Println(err) // fail to delete record
    }
```

### Transaction

```go
    // Example
    if err := db.RunInTransaction(func(txn *goloquent.DB) error {
        user := new(User)
        if err := txn.Create(user, nil); err != nil {
            return err // return any err to rollback the transaction
        }
        return nil // return nil to commit the transaction
    }); err != nil {
        log.Println(err)
    }
```

- **Table Locking (only effective inside RunInTransaction)**

```go
    // Example
    merchantKey := datastore.IDKey("Merchant", "mjfFgYnxBS", nil)
    userKey := datastore.IDKey("User", int64(4645436182170916864), nil)
    if err := db.RunInTransaction(func(txn *goloquent.DB) error {
        user := new(User)

        if err := txn.NewQuery().
            WLock(). // Lock record for update
            Find(userKey, user); err != nil {
            return err
        }

        merchant := new(Merchant)
        if err := txn.NewQuery().
            RLock(). // Lock record for read
            Find(merchantKey, merchant); err != nil {
            return err
        }

        user.Age = 30
        if err := txn.Save(user); err != nil {
            return err // return any err to rollback the transaction
        }

        return nil // return nil to commit the transaction
    }); err != nil {
        log.Println(err)
    }
```

- **Database Migration**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example
    user := new(User)
    if err := db.Migrate(
        new(user),
        Merchant{},
        &Log{},
    ); err != nil {
        log.Println(err)
    }
```

- **Filter Query**

```go
    import "github.com/si3nloong/goloquent/db"
    // Update single record
    user := new(User)
    if err := db.NewQuery().
        WhereIn("Status", []interface{}{"active", "pending"}).
        First(user); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }

    // Get record with like
    if err := db.NewQuery().
        WhereLike("Name", "%name%").
        First(user); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }
```

- **Update Query**

```go
    import "github.com/si3nloong/goloquent/db"
    // Update multiple record
    if err := db.Table("User").
        Where("Age", ">", 10).
        Update(map[string]interface{}{
            "Name": "New Name",
            "Email": "email@gmail.com",
            "UpdatedDateTime": time.Now().UTC(),
        }); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }

    if err := db.Table("User").
        Omit("Name").
        Where("Age", ">", 10).
        Update(User{
            Name: "New Name",
            Email: "test@gmail.com",
            UpdatedDateTime: time.Now().UTC(),
        }); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }
```

- **JSON Filter**

```go
    import "github.com/si3nloong/goloquent/db"

    // JSON equal
    users := new([]User)
    postCode := uint32(63000)
	if err := db.NewQuery().
		WhereJSONEqual("Address>PostCode", &postCode).
		Get(users); err != nil {
		log.Println(err)
    }

    // JSON not equal
    var timeZone *time.Time
	if err := db.NewQuery().
		WhereJSONNotEqual("Address>region.TimeZone", timeZone).
		Get(users); err != nil {
		log.Println(err)
    }

    // JSON contains any
    if err := db.NewQuery().
		WhereJSONContainAny("Nicknames", []string{
            "Joe", "John", "Robert",
        }).Get(users); err != nil {
		log.Println(err)
    }

    // JSON check type
    if err := db.NewQuery().
		WhereJSONType("Address>region", "Object").
        Get(users); err != nil {
		log.Println(err)
    }

    // JSON check is object type
    if err := db.NewQuery().
		WhereJSONIsObject("Address>region").
        Get(users); err != nil {
		log.Println(err)
    }

    // JSON check is array type
    if err := db.NewQuery().
		WhereJSONIsArray("Address>region.keys").
        Get(users); err != nil {
		log.Println(err)
    }
```

- **Data Type Support for Where Filtering**

The supported data type are :

```go
- string
- bool
- int, int8, int16, int32 and int64 (signed integers)
- uint, uint8, uint16, uint32 and uint64
- float32 and float64
- []byte
- datastore.GeoPoint
- goloquent.Date
- json.RawMessage
- time.Time
- pointers to any one of the above
- *datastore.Key
- slices of any of the above
```

- **Extra Schema Option**

Available shorthand:

- longtext (only applicable for `string` data type)
- index
- unsigned (only applicable for `float32` and `float64` data type)
- flatten (only applicable for struct or []struct)

```go
type model struct {
    CreatedDateTime time.Time // `CreatedDateTime`
    UpdatedDateTime time.Time // `UpdatedDateTime`
}

// Fields may have a `goloquent:"name,options"` tag.
type User struct {
    Key         *datastore.Key `goloquent:"__key__"` // Primary Key
    Name        string `goloquent:",longtext"` // Using `TEXT` datatype instead of `VARCHAR(255)` by default
    CreditLimit    float64    `goloquent:",unsigned"` // Unsigned option only applicable for float32 & float64 data type
    PhoneNumber string `goloquent:",charset=utf8,collate=utf8_bin,datatype=char(20)"`
    Email       string
    Skip        string `goloquent:"-"` // Skip this field to store in db
    DefaultAddress struct {
        AddressLine1 string // `DefaultAddress.AddressLine1`
        AddressLine2 string // `DefaultAddress.AddressLine2`
        PostCode     int    // `DefaultAddress.PostCode`
        City         string // `DefaultAddress.City`
        State        string // `DefaultAddress.State`
        Country      string
    } `goloquent:",flatten"` // Flatten the struct field
    Birthdate *goloquent.Date
    ExtraInfo json.RawMessage
    model                    // Embedded struct
    Deleted goloquent.SoftDelete
}
```

The supported data type are :

```go
- string
- int, int8, int16, int32 and int64 (signed integers)
- uint, uint8, uint16, uint32 and uint64
- bool
- float32 and float64
- []byte
- any type whose underlying type is one of the above predeclared types
- datastore.GeoPoint
- goloquent.Date
- goloquent.SoftDelete
- time.Time
- json.RawMessage
- structs whose fields are all valid value types
- pointers to any one of the above
- *datastore.Key
- slices of any of the above
```

| Data Type          | Mysql               | Postgres            | Default Value       | CharSet |
| :----------------- | :------------------ | ------------------- | :------------------ | :------ |
| \*datastore.Key    | varchar(512)        | varchar(512)        |                     | latin1  |
| datastore.GeoPoint | varchar(50)         | varchar(50)         | {Lat: 0, Lng: 0}    |         |
| string             | varchar(191)        | varchar(191)        | ""                  | utf8mb4 |
| []byte             | mediumblob          | bytea               |                     |         |
| bool               | boolean             | bool                | false               |         |
| float32            | double              | real                | 0                   |         |
| float64            | double              | real                | 0                   |         |
| int                | int                 | integer             | 0                   |         |
| int8               | tinyint             | smallint            | 0                   |         |
| int16              | smallint            | smallint            | 0                   |         |
| int32              | mediumint           | integer             | 0                   |         |
| int64              | bigint              | bigint              | 0                   |         |
| uint               | int (unsigned)      | integer (unsigned)  | 0                   |         |
| uint8              | smallint (unsigned) | smallint (unsigned) | 0                   |         |
| uint16             | smallint (unsigned) | smallint (unsigned) | 0                   |         |
| uint32             | smallint (unsigned) | integer (unsigned)  | 0                   |         |
| uint64             | bigint (unsigned)   | bigint (unsigned)   | 0                   |         |
| slice or array     | json                | jsonb               |                     |         |
| struct             | json                | jsonb               |                     |         |
| json.RawMessage    | json                | jsonb               |                     |         |
| Date               | date                | date                | 0001-01-01          |         |
| time.Time          | datetime            | timestamp           | 0001-01-01 00:00:00 |         |
| SoftDelete         | datetime (nullable) | timestamp           | NULL                |         |

**$Key**, **$Deleted** are reserved words, please avoid to use these words as your column name

[MIT License](https://github.com/si3nloong/goloquent/blob/master/LICENSE)
