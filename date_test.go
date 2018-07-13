package goloquent

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDate(t *testing.T) {
	var i struct {
		Birthdate Date `json:"birthdate"`
	}
	if err := json.Unmarshal([]byte(`{"birthdate":"2006-01-02"}`), &i); err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal([]byte(`{"birthdate":"2006-21-02"}`), &i); err == nil {
		t.Fatal(fmt.Errorf("Unexpected result, Date should error"))
	}
}
