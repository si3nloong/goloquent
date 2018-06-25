package goloquent

import (
	"fmt"
	"reflect"
	"testing"
)

func TestStructTagWithSkip(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := newTag(vt.Field(0))
	if !tag.isSkip() {
		t.Fatal(fmt.Sprintf("Expected tag have skip, but end up with %v", tag.name))
	}

}

func TestStructTagWithCharSet(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := newTag(vt.Field(2))
	if tag.Get("charset") != "latin1" {
		t.Fatal(fmt.Sprintf("Expected tag have %q charset, but end up with %v", "latin1", tag.Get("charset")))
	}
}

func TestStructTagWithLongText(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := newTag(vt.Field(5))
	if !tag.IsLongText() {
		t.Fatal("Expected tag have longtext, but end up with no longtext")
	}
}

func TestStructTagWithIndex(t *testing.T) {
	var i testUser
	vt := reflect.ValueOf(i).Type()
	tag := newTag(vt.Field(3))
	if !tag.IsIndex() {
		t.Fatal("Expected tag have index, but end up with noindex")
	}
}
