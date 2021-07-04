package goloquent

import (
	"errors"
	"log"
	"reflect"
)

func getCodec(t reflect.Type) (*structCodec, error) {
	if t.Kind() != reflect.Struct {
		return nil, errors.New("goloquent: model must be a struct")
	}

	codec := new(structCodec)
	queue := []structScan{{[]int{}, []int{}, t, nil, false, codec}}
	for len(queue) > 0 {
		first := queue[0]
		st := first.typeOf

		// fields := first.structCodec.fields
		numOfField := st.NumField()
		for i := 0; i < numOfField; i++ {
			sf := st.Field(i)

			parseTag(sf)
			log.Println(sf)
		}
		queue = queue[1:]
	}
	return codec, nil
}
