package goloquent

import (
	"log"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	code, err := getCodec(reflect.TypeOf(User{}))
	require.NoError(t, err)

	log.Println(code)
	panic("")
}
