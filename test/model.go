package test

import (
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/bxcodec/faker"
	"github.com/si3nloong/goloquent"
)

var (
	my        *goloquent.DB
	nameKey   = datastore.NameKey("Name", "hIL0O7zfZP", nil)
	symbolKey = datastore.NameKey("Name", "VEknB=YnisrgS0w'9Hg,TWpSQtg7w/b0recIBLkjp+lf", nil)
	idKey     = datastore.IDKey("ID", int64(5116745034367558422), nil)
)

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

// Email :
type Email string

// User :
type User struct {
	ID               int64
	Key              *datastore.Key `goloquent:"__key__" faker:"-"`
	Username         string         `faker:"username"`
	Name             string         `goloquent:",charset=utf8,collate=utf8_bin" faker:"name"`
	Password         []byte         `goloquent:",datatype=varchar(100)" faker:"password"`
	Nickname         *string
	Age              uint8            ``
	CreditLimit      float64          `goloquent:",unsigned"`
	Address          Address          `faker:"-"`
	Birthdate        goloquent.Date   `faker:"-"`
	PrimaryEmail     Email            `faker:"email"`
	Emails           []string         `goloquent:"" faker:"email"`
	Information      json.RawMessage  `faker:"-"`
	ExtraInformation *json.RawMessage `faker:"-"`
	Status           string           `goloquent:",charset=latin1" faker:""`
	UpdatedDateTime  time.Time
	DeleteDateTime   goloquent.SoftDelete `faker:"-"`
}

// TempUser :
type TempUser struct {
	User
}

func getFakeUser() *User {
	u := new(User)
	faker.FakeData(u)
	u.Username = fmt.Sprintf("%d", time.Now().UnixNano())
	u.Birthdate = goloquent.Date(time.Now())
	u.Information = json.RawMessage(`{"nickname":"John Doe"}`)
	u.Address.Line1 = "7812, Jalan Section 22"
	u.Emails = []string{"support@hotmail.com", "support@gmail.com"}
	u.Status = "ACTIVE"
	return u
}
