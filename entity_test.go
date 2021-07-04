package goloquent

import (
	"log"
	"testing"

	"cloud.google.com/go/datastore"
)

type User struct {
	Key *datastore.Key `datastore:"__key__"`
}

func TestEntity(t *testing.T) {
	var (
		nilUser *User
		ent     *entity
		err     error
	)

	ent, err = newEntity(nilUser)
	log.Println(ent, err)
	ent, err = newEntity(nil)
	log.Println(ent, err)
	ent, err = newEntity(&[]User{})
	log.Println(ent, err)
	ent, err = newEntity(&User{})
	log.Println(ent, err)
}
