package goloquent

import (
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
)

type Nested struct {
	testUser
}

type TestModel struct {
	CreatedAt time.Time
	UpdatedAt time.Time
}

type TestString string

type testUser struct {
	ID          string         `goloquent:"-"`
	Key         *datastore.Key `goloquent:"__key__"`
	Name        TestString     `goloquent:",charset=latin1"`
	Username    string         `goloquent:",index"`
	Password    []byte         `goloquent:"Secret"`
	Biography   TestString     `goloquent:",longtext"`
	priv        *bool
	Nickname    []string ``
	Age         uint8
	CreditLimit float64 `goloquent:",unsigned"`
	Addresses   []struct {
		AddressLine1 string
		AddressLine2 *string
		PostCode     uint32
		Country      string
	}
	// House       []*datastore.Key
	IsSingle    bool
	LastLoginAt *time.Time
	TestModel
	DeleteAt SoftDelete
}

func (x *testUser) Load() error {
	x.ID = StringKey(x.Key)
	return nil
}

func (x *testUser) Save() error {
	// x.CreditLimit = 0
	return nil
}

type testCodec struct {
	name       string
	paths      []int
	isPtrChild bool
	isIndex    bool
	isSlice    bool
}

func TestStructCodec(t *testing.T) {
	var i testUser
	cc, err := getStructCodec(&i)
	if err != nil {
		log.Fatal("Expected error free, but instead err :", err)
	}

	list := []testCodec{
		testCodec{"__key__", []int{1}, false, false, false},
		testCodec{"Name", []int{2}, false, false, false},
		testCodec{"Username", []int{3}, false, true, false},
		testCodec{"Secret", []int{4}, false, false, false},
		testCodec{"Biography", []int{5}, false, false, false},
		testCodec{"Nickname", []int{7}, false, false, true},
		testCodec{"Age", []int{8}, false, false, false},
		testCodec{"CreditLimit", []int{9}, false, false, false},
		testCodec{"Addresses", []int{10}, false, false, true},
		testCodec{"IsSingle", []int{11}, false, false, false},
		testCodec{"LastLoginAt", []int{12}, false, false, false},
		testCodec{"CreatedAt", []int{13, 0}, false, false, false},
		testCodec{"UpdatedAt", []int{13, 1}, false, false, false},
		testCodec{"$Deleted", []int{14}, false, false, false},
	}

	if len(list) != len(cc.fields) {
		log.Fatal("Unmatched number of property")
	}

	for i, f := range cc.fields {
		ff := list[i]
		if ff.name != f.name {
			log.Fatal(fmt.Sprintf("Unexpected property name value, expected %q, but get %q", ff.name, f.name))
		}
		if ff.isIndex != f.IsIndex() {
			log.Fatal(fmt.Sprintf("Unexpected property tag value, expected %v, but get %v", ff.isIndex, f.IsIndex()))
		}
		if ff.isSlice != f.isSlice() {
			log.Fatal(fmt.Sprintf("Unexpected property data type value, expected `slice`, but get %v", f.typeOf))
		}
		if ff.isPtrChild != f.isPtrChild {
			log.Fatal(fmt.Sprintf("Unexpected property ptr child value, expected %v, but get %v", ff.isPtrChild, f.isPtrChild))
		}
		if !reflect.DeepEqual(ff.paths, f.paths) {
			log.Fatal(fmt.Sprintf("Unexpected property index paths, expected %v, but get %v", ff.paths, f.paths))
		}
	}

	var j Nested
	_, err = getStructCodec(&j)
	if err != nil {
		log.Fatal("Expected error free, but instead err :", err)
	}
}
