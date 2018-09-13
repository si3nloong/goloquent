package goloquent

import (
	"fmt"
	"math/rand"
	"net/url"
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
		vv = strings.TrimSpace(vv)
		if vv == "" {
			continue
		}
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
	if key == nil {
		return ""
	}
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
	id := rand.Int63n(maxSeed-minSeed) + minSeed
	if parentKey != nil && parentKey.Kind == table {
		parentKey.ID = id
		return parentKey
	}

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

// ParseKey :
func ParseKey(str string) (*datastore.Key, error) {
	return parseKey(str)
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
			name, err := url.PathUnescape(strings.Trim(value, `'`))
			if err != nil {
				return nil, err
			}
			key.Name = name
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
			name := url.PathEscape(parentKey.Name)
			k = fmt.Sprintf(`%s,'%s'`, parentKey.Kind, name)
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
	if k == nil {
		return "", ""
	}
	if k.ID > 0 {
		return fmt.Sprintf("%d", k.ID), stringifyKey(k.Parent)
	}
	name := url.PathEscape(k.Name)
	return fmt.Sprintf(`'%s'`, name), stringifyKey(k.Parent)
}

func stringPk(k *datastore.Key) string {
	kk, pp := splitKey(k)
	return strings.Trim(fmt.Sprintf("%s%s%s", pp, keyDelimeter, kk), keyDelimeter)
}

// compareVersion: is compare using semantic versioning
// if a > b, result will be -1
// if b < a, result will be 1
// if a = b, result will be 0
func compareVersion(a, b string) (ret int) {
	as := strings.Split(a, ".")
	bs := strings.Split(b, ".")
	loopMax := len(bs)
	if len(as) > len(bs) {
		loopMax = len(as)
	}
	for i := 0; i < loopMax; i++ {
		var x, y string
		if len(as) > i {
			x = as[i]
		}
		if len(bs) > i {
			y = bs[i]
		}
		xi, _ := strconv.Atoi(x)
		yi, _ := strconv.Atoi(y)
		if xi > yi {
			ret = -1
		} else if xi < yi {
			ret = 1
		}
		if ret != 0 {
			break
		}
	}
	return
}

func escapeSingleQuote(v string) string {
	return strings.Replace(v, `'`, `''`, -1)
}
