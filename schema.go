package goloquent

var (
	utf8CharSet    = &CharSet{"utf8", "utf8_unicode_ci"}
	utf8mb4CharSet = &CharSet{"utf8mb4", "utf8mb4_unicode_ci"}
	latin2CharSet  = &CharSet{"latin2", "latin2_general_ci"}
)

type omitDefault interface{}

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
	*CharSet
}
