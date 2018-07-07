package goloquent

import (
	"fmt"
	"reflect"
	"time"
)

var typeOfDate = reflect.TypeOf(Date(time.Time{}))

// Date :
type Date time.Time

// UnmarshalJSON :
func (d *Date) UnmarshalJSON(b []byte) error {
	dt, err := time.Parse("2006-01-02", string(b))
	if err != nil {
		return err
	}
	*d = Date(dt)
	return nil
}

// MarshalJSON :
func (d Date) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", time.Time(d).Format("2006-01-02"))), nil
}

// String :
func (d Date) String() string {
	return time.Time(d).Format("2006-01-02")
}
