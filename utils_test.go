package goloquent

import (
	"testing"

	"cloud.google.com/go/datastore"
)

const errUnexpectedResult = `function %q produce unexpected result`

func TestUtils(t *testing.T) {
	// if isNameKey("Table1,'email'/Table2,9100222") {
	// 	t.Errorf("invalid expected result")
	// }

	// if !isNameKey("Table1,'email'/Table2,9100222") {
	// 	t.Errorf("invalid expected result")
	// }

	idKey := datastore.IDKey("Kind", int64(192839128), nil)
	if stringifyKey(idKey) != "Kind,192839128" {
		t.Errorf(errUnexpectedResult, "stringifyKey")
	}
	if StringKey(idKey) != "192839128" {
		t.Errorf(errUnexpectedResult, "StringKey")
	}

	nameKey := datastore.NameKey("Kind", "test@hotmail.com", nil)
	if stringifyKey(nameKey) != "Kind,'test@hotmail.com'" {
		t.Errorf(errUnexpectedResult, "stringifyKey")
	}
	if StringKey(nameKey) != "test@hotmail.com" {
		t.Errorf(errUnexpectedResult, "StringKey")
	}
}
