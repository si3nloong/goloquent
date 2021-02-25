package goloquent

import (
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"
)

// A :
type A struct {
	Key *datastore.Key `datastore:"__key__"`
}

func TestCode(t *testing.T) {
	codecByType(reflect.TypeOf(A{}))
}
