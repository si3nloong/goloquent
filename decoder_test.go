package goloquent

import (
	"fmt"
	"testing"
)

func TestEscape(t *testing.T) {
	txt := "hello word"
	if escape([]byte(fmt.Sprintf(`"%s"`, txt))) != txt {
		t.Errorf(errUnexpectedResult, "escape")
	}
}

func TestInitStruct(t *testing.T) {

}

func TestIterator(t *testing.T) {
	// var i testUser

	// email := `test@hotmail.com`
	// it := &Iterator{}
	// it.put(0, "Email", []byte(email))
	// it.put(0, "Age", []byte(`100`))
	// it.put(0, "IsSingle", nil)

	// if err := it.Scan(&i); err != nil {
	// 	t.Errorf("")
	// }

	// if i.Email != email {
	// 	t.Error()
	// }
	// fmt.Println(i)
}

func TestValueToInterface(t *testing.T) {
	// var i testUser
	// vt := reflect.TypeOf(i)
	// vv, _ := valueToInterface(vt.Field(0).Type, []byte(`178330303`), true)
	// if vv != "178330303" {
	// 	log.Fatal(fmt.Sprintf("Unexpected value using valueToInterface %v", vv))
	// }

	// vv, _ = valueToInterface(vt.Field(2).Type, []byte(`Joe`), true)
	// if vv != "Joe" {
	// 	log.Fatal(fmt.Sprintf("Unexpected value using valueToInterface %v", vv))
	// }
}

func TestLoadStructField(t *testing.T) {}

func TestLoadField(t *testing.T) {

}
