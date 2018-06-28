package goloquent

const (
	defaultLimit = 100
)

// Pagination :
type Pagination struct {
	query     *Query
	Cursor    string
	Limit     uint
	count     uint
	nxtCursor Cursor
}

// SetQuery :
func (p *Pagination) SetQuery(q *Query) {
	if q == nil {
		return
	}
	p.query = q
}

// Reset : reset all the value in pagination to default value
func (p *Pagination) Reset() {
	pp := new(Pagination)
	pp.Limit = defaultLimit
	*p = *pp
}

// NextCursor : next record set cursor
func (p *Pagination) NextCursor() string {
	return p.nxtCursor.String()
}

// Count : record count in this pagination record set
func (p *Pagination) Count() uint {
	return p.count
}
