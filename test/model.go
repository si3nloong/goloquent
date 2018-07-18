package test

import (
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/bxcodec/faker"
	"github.com/si3nloong/goloquent"
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

// User :
type User struct {
	Key             *datastore.Key `goloquent:"__key__" faker:"-"`
	Username        string         `faker:"username"`
	Name            string         `goloquent:",charset=utf8,collate=utf8_bin" faker:"name"`
	Password        string         `goloquent:",datatype=varchar(100)" faker:"password"`
	Age             uint           ``
	CreditLimit     float64        `goloquent:",unsigned"`
	Address         Address        `faker:"-"`
	Birthdate       goloquent.Date `faker:"-"`
	PrimaryEmail    string         `faker:"email"`
	Email           []string       `goloquent:"" faker:"email"`
	Status          string         `goloquent:",charset=latin1" faker:""`
	UpdatedDateTime time.Time
	DeleteDateTime  goloquent.SoftDelete `faker:"-"`
}

func getFakeUser() *User {
	u := new(User)
	faker.FakeData(u)
	u.Username = fmt.Sprintf("%d", time.Now().UnixNano())
	u.Birthdate = goloquent.Date(time.Now())
	u.Age = 85
	u.Address.Line1 = "7812, Jalan Section 22"
	u.Email = []string{"support@hotmail.com", "support@gmail.com"}
	u.Status = "ACTIVE"
	return u
}
