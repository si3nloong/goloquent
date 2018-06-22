package goloquent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/datastore"
)

// Cursor :
type Cursor struct {
	cc        []byte
	Signature string         `json:"signature"`
	Key       *datastore.Key `json:"next"`
}

// String :
func (c Cursor) String() string {
	if c.cc == nil {
		return ""
	}
	return strings.TrimRight(base64.URLEncoding.EncodeToString(c.cc), "=")
}

// DecodeCursor :
func DecodeCursor(c string) (Cursor, error) {
	if c == "" {
		return Cursor{}, nil
	}
	if n := len(c) % 4; n != 0 {
		c += strings.Repeat("=", 4-n)
	}
	b, err := base64.URLEncoding.DecodeString(c)
	if err != nil {
		return Cursor{}, fmt.Errorf("goloquent: invalid cursor")
	}
	cc := new(Cursor)
	cc.cc = b
	if err := json.Unmarshal(b, cc); err != nil {
		return Cursor{}, fmt.Errorf("goloquent: invalid cursor")
	}
	return *cc, nil
}
