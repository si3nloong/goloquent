package goloquent

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var typeOfDate = reflect.TypeOf(Date(time.Time{}))

// Date :
type Date time.Time

// UnmarshalJSON :
func (d *Date) UnmarshalJSON(b []byte) error {
	rgx := regexp.MustCompile(`^\"\d{4}\-\d{2}\-\d{2}\"$`)
	if !rgx.Match(b) {
		return fmt.Errorf("goloquent: invalid date value %q", b)
	}
	dt, err := time.Parse("2006-01-02", strings.Trim(string(b), `"`))
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
