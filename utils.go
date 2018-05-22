package goloquent

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

// Dictionary :
type dictionary map[string]bool

func newDictionary(v []string) dictionary {
	l := make(map[string]bool)
	for _, vv := range v {
		l[vv] = true
	}
	return dictionary(l)
}

func (d dictionary) add(k string) {
	if !d.has(k) {
		d[k] = true
	}
}

// Has :
func (d dictionary) has(k string) bool {
	return d[k]
}

// Delete :
func (d dictionary) delete(k string) {
	delete(d, k)
}

// Keys :
func (d dictionary) keys() []string {
	arr := make([]string, 0, len(d))
	for k := range d {
		arr = append(arr, k)
	}
	return arr
}

// StringKey :
func StringKey(key *datastore.Key) string {
	if key.Name != "" {
		return key.Name
	}
	return fmt.Sprintf("%d", key.ID)
}

// minimum and maximum value for random seed
const (
	minSeed = int64(1000000)
	maxSeed = int64(9223372036854775807)
)

// newPrimaryKey will generate a new key if the key provided was incomplete
// and it will ensure the key will not be incomplete
func newPrimaryKey(table string, parentKey *datastore.Key) *datastore.Key {
	if parentKey != nil && ((parentKey.Kind == table && parentKey.Name != "") ||
		(parentKey.Kind == table && parentKey.ID > 0)) {
		return parentKey
	}

	rand.Seed(time.Now().UnixNano())
	var id = rand.Int63n(maxSeed-minSeed) + minSeed
	key := new(datastore.Key)
	key.Kind = table
	key.ID = id
	if parentKey != nil {
		key.Parent = parentKey
	}
	return key
}

func isNameKey(strKey string) bool {
	if strKey == "" {
		return false
	}
	if strings.HasPrefix(strKey, "name=") {
		return true
	}
	_, err := strconv.ParseInt(strKey, 10, 64)
	if err != nil {
		return true
	}
	paths := strings.Split(strKey, "/")
	if len(paths) != 2 {
		return strings.HasPrefix(strKey, "'") && strings.HasSuffix(strKey, "'")
	}
	lastPath := strings.Split(paths[len(paths)-1], ",")[1]
	return strings.HasPrefix(lastPath, "'") || strings.HasSuffix(lastPath, "'")
}

// parseKey will parse any key string to *datastore.Key,
// it will return null *datastore.Key if the key string is empty
func parseKey(str string) (*datastore.Key, error) {
	str = strings.Trim(strings.TrimSpace(str), `"`)
	if str == "" {
		var k *datastore.Key
		return k, nil
	}

	paths := strings.Split(strings.Trim(str, "/"), "/")
	parentKey := new(datastore.Key)
	for _, p := range paths {
		path := strings.Split(p, ",")
		if len(path) != 2 {
			return nil, fmt.Errorf("goloquent: incorrect key value: %q, suppose %q", p, "table,value")
		}

		kind, value := path[0], path[1]
		if kind == "" || value == "" {
			return nil, fmt.Errorf("goloquent: invalid key value format: %q, suppose %q", p, "table,value")
		}
		key := new(datastore.Key)
		key.Kind = kind
		if isNameKey(value) {
			key.Name = strings.Trim(value, `'`)
		} else {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("goloquent: incorrect key id, %v", value)
			}
			key.ID = n
		}

		if !parentKey.Incomplete() {
			key.Parent = parentKey
		}
		parentKey = key
	}

	return parentKey, nil
}

// StringifyKey :
func StringifyKey(key *datastore.Key) string {
	return stringifyKey(key)
}

// stringifyKey, will transform key to either string or empty string
func stringifyKey(key *datastore.Key) string {
	paths := make([]string, 0)
	parentKey := key

	for parentKey != nil {
		var k string
		if parentKey.Name != "" {
			k = fmt.Sprintf(`%s,'%s'`, parentKey.Kind, parentKey.Name)
		} else {
			k = fmt.Sprintf("%s,%d", parentKey.Kind, parentKey.ID)
		}
		paths = append([]string{k}, paths...)
		parentKey = parentKey.Parent
	}

	if len(paths) > 0 {
		return strings.Join(paths, "/")
	}

	return ""
}

func splitKey(k *datastore.Key) (key string, parent string) {
	if k.ID > 0 {
		return fmt.Sprintf("%d", k.ID), stringifyKey(k.Parent)
	}
	return fmt.Sprintf(`'%s'`, k.Name), stringifyKey(k.Parent)
}