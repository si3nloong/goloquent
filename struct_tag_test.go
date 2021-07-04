package goloquent

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStructTagWithSkip(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := parseTag(vt.Field(0))
	if !tag.IsSkip() {
		t.Fatal(fmt.Sprintf("Expected tag have skip, but end up with %v", tag.name))
	}
}

func TestStructTagWithCharSet(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := parseTag(vt.Field(2))
	v, _ := tag.Lookup("charset")
	require.True(t, v == "latin1")
}

func TestStructTagWithLongText(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := parseTag(vt.Field(5))
	if !tag.IsLongText() {
		t.Fatal("Expected tag have longtext, but end up with no longtext")
	}
}

func TestStructTagWithIndex(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := parseTag(vt.Field(3))
	if !tag.IsIndex() {
		t.Fatal("Expected tag have index, but end up with noindex")
	}
}
