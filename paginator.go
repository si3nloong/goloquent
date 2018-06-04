package goloquent

const (
	defaultLimit = 100
)

// Pagination :
type Pagination struct {
	query  *Query
	Cursor string
	Limit  uint
	count  uint
}

// SetQuery :
func (p *Pagination) SetQuery(q *Query) {
	p.query = q
}

// Next :
func (p *Pagination) Next() bool {
	return p.Cursor != ""
}

// Reset :
func (p *Pagination) Reset() {
	pp := new(Pagination)
	pp.Limit = defaultLimit
	*p = *pp
}

// Count :
func (p *Pagination) Count() uint {
	return p.count
}
