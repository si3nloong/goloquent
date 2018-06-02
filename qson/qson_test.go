package qson

import (
	"fmt"
	"testing"
)

type testUser struct {
	Email      string `json:"email"`
	private    string
	Skip       string `json:"-"`
	Credential struct {
		Hash     string `json:"hash"`
		Salt     string `json:"salt"`
		Password string `json:"password"`
	} `json:"credential"`
	Address []struct {
		Line1 string `json:"line1"`
	} `json:"address"`
	Status string `json:"status"`
}

// QSON : Query JSON

func TestProperty(t *testing.T) {
	var i testUser
	parser, err := New(i)
	if err != nil {
		return
	}

	fields, err := parser.Parse([]byte(`{
		"email":"sianloong90@gmail.com",
		"status":{"$in":["OK","FAILED"]}
	}`))
	for _, ff := range fields {
		fmt.Println(ff.Name(), ff.Operator(), ff.Value())
	}
	sorts, err := parser.ParseSort([]string{
		"-credential.password",
		"email",
	})
	fmt.Println(sorts, err)

	for _, ss := range sorts {
		fmt.Println(ss.Name(), ss.IsAscending())
	}
}
