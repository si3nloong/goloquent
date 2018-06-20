package goloquent

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

type sorter struct {
	order
	Value interface{} `json:"value"`
}

type cursor struct {
	Kind   string   `json:"kind"`
	Filter []string `json:"filter"`
	Sort   []sorter `json:"sort"`
	Cursor string   `json:"cursor"`
}

// Cursor :
type Cursor struct {
	cc []byte
}

func (c Cursor) offset() int32 {
	i, _ := strconv.Atoi(strings.Replace(string(c.cc), "offset=", "", -1))
	return int32(i)
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
	return Cursor{b}, nil
}
