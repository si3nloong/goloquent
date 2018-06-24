package test

import (
	"time"

	"cloud.google.com/go/datastore"
	"github.com/bxcodec/faker"
	"github.com/si3nloong/goloquent"
)

// User :
type User struct {
	Key             *datastore.Key `goloquent:"__key__" faker:"-"`
	Name            string         `goloquent:",charset=utf8,collate=utf8_bin" faker:"name"`
	Password        string         `goloquent:",datatype=varchar(100)" faker:"password"`
	Age             uint           ``
	CreditLimit     float64        `goloquent:",unsigned"`
	Address         string         `goloquent:",longtext"`
	Email           []string       `goloquent:"" faker:"email"`
	Status          string         `goloquent:",charset=latin1" faker:""`
	UpdatedDateTime time.Time
	DeleteDateTime  goloquent.SoftDelete `faker:"-"`
}

func getFakeUser() *User {
	u := new(User)
	faker.FakeData(u)
	return u
}
