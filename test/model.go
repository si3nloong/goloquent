package test

import (
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/bxcodec/faker"
	"github.com/si3nloong/goloquent"
)

// User :
type User struct {
	Key         *datastore.Key `goloquent:"__key__" faker:"-"`
	Username    string         `faker:"username"`
	Name        string         `goloquent:",charset=utf8,collate=utf8_bin" faker:"name"`
	Password    string         `goloquent:",datatype=varchar(100)" faker:"password"`
	Age         uint           ``
	CreditLimit float64        `goloquent:",unsigned"`
	Address     struct {
		Line1    string
		Line2    string
		Country  string
		PostCode uint
	}
	Birthdate       goloquent.Date `faker:"-"`
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
	return u
}
