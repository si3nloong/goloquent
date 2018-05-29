package goloquent

const (
	defaultLimit = 100
)

// Pagination :
type Pagination struct {
	Cursor string
	Filter []Filter
	Sort   []string
	Limit  uint
	count  uint
}

// Reset :
func (p *Pagination) Reset() {
	pp := new(Pagination)
	pp.Limit = defaultLimit
	p = pp
}

// Count :
func (p *Pagination) Count() uint {
	return p.count
}
