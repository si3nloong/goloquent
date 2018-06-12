package test

import "github.com/si3nloong/goloquent/db"

func init() {
	_, err := db.Open("mysql", db.Config{})
	if err != nil {
		panic(err)
	}
}
