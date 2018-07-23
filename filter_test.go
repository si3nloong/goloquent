package goloquent

import (
	"reflect"
	"testing"
)

func TestNormalizeValue(t *testing.T) {
	var (
		i   interface{}
		err error
	)

	i64 := int64(16)
	i, err = normalizeValue(int8(16))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, i64) {
		t.Fatal("Unexpected error on normalize integer")
	}
	i8 := int8(16)
	i, err = normalizeValue(&i8)
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(reflect.Indirect(reflect.ValueOf(i)).Interface(), i64) {
		t.Fatal("Unexpected error on normalize integer")
	}

	i, err = normalizeValue(int16(16))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, i64) {
		t.Fatal("Unexpected error on normalize integer")
	}

	i, err = normalizeValue(int(16))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, i64) {
		t.Fatal("Unexpected error on normalize integer")
	}

	i, err = normalizeValue(int32(16))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, i64) {
		t.Fatal("Unexpected error on normalize integer")
	}

	ui64 := uint64(55)
	i, err = normalizeValue(uint(55))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, ui64) {
		t.Fatal("Unexpected error on normalize unsigned integer")
	}

	i, err = normalizeValue(uint8(55))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, ui64) {
		t.Fatal("Unexpected error on normalize unsigned integer")
	}

	i, err = normalizeValue(uint16(55))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, ui64) {
		t.Fatal("Unexpected error on normalize unsigned integer")
	}

	i, err = normalizeValue(uint32(55))
	if err != nil {
		t.Fatalf("Unexpected err, %v", err)
	}
	if !reflect.DeepEqual(i, ui64) {
		t.Fatal("Unexpected error on normalize unsigned integer")
	}

}
