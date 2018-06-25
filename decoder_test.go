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
	// i := new(testUser)
	// v := reflect.Indirect(reflect.ValueOf(i))
	// initStruct(v.FieldByName("Nested"))
	// if i.Nested == nil {
	// 	t.Errorf("initStruct result is unexpected")
	// }
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

}

func TestLoadStructField(t *testing.T) {}

func TestLoadField(t *testing.T) {

}
