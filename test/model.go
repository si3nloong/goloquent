package test

import (
	"cloud.google.com/go/datastore"
	"github.com/si3nloong/goloquent"
)

// User :
type User struct {
	Key            *datastore.Key `goloquent:"__key__"`
	Name           string
	Password       string `goloquent:""`
	Status         string `goloquent:",charset=latin1"`
	DeleteDateTime goloquent.SoftDelete
}

var user User

func init() {
	user.Name = "Joe"
	user.Status = "ACTIVE"
}
