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
