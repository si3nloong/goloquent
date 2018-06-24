package goloquent

import "reflect"

var (
	utf8CharSet    = CharSet{"utf8", "utf8_unicode_ci"}
	utf8mb4CharSet = CharSet{"utf8mb4", "utf8mb4_unicode_ci"}
	latin2CharSet  = CharSet{"latin2", "latin2_general_ci"}
	latin1CharSet  = CharSet{"latin1", "latin1_bin"}
)

// OmitDefault :
type OmitDefault interface{}

// CharSet :
type CharSet struct {
	Encoding  string
	Collation string
}

// Schema :
type Schema struct {
	Name         string
	DataType     string
	DefaultValue interface{}
	IsUnsigned   bool
	IsNullable   bool
	IsIndexed    bool
	CharSet
}

// IsOmitEmpty :
func (s Schema) IsOmitEmpty() bool {
	return reflect.TypeOf(s.DefaultValue) == reflect.TypeOf(OmitDefault(nil))
}
