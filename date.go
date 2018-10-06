package goloquent

import (
	"fmt"
	"reflect"
	"regexp"
	"time"
)

var typeOfDate = reflect.TypeOf(Date(time.Time{}))

// Date :
type Date time.Time

// UnmarshalJSON :
func (d *Date) UnmarshalJSON(b []byte) error {
	if b == nil || b2s(b) == "null" {
		return nil
	}
	if !regexp.MustCompile(`^\"\d{4}\-\d{2}\-\d{2}\"$`).Match(b) {
		return fmt.Errorf("goloquent: invalid date value %q", b)
	}
	dt, err := time.Parse("2006-01-02", escape(b))
	if err != nil {
		return err
	}
	*d = Date(dt)
	return nil
}

// MarshalJSON :
func (d Date) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(d).Format("2006-01-02") + `"`), nil
}

// UnmarshalText :
func (d *Date) UnmarshalText(b []byte) error {
	dt, err := time.Parse("2006-01-02", b2s(b))
	if err != nil {
		return err
	}
	*d = Date(dt)
	return nil
}

// MarshalText :
func (d Date) MarshalText() ([]byte, error) {
	return []byte(time.Time(d).Format("2006-01-02")), nil
}

// String :
func (d Date) String() string {
	return time.Time(d).Format("2006-01-02")
}
