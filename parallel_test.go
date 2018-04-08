package jsoniterll

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

var bulk []interface{}

func init() {
	check(unmarshalFile("testdata/people.json", &bulk))
}

func TestJsoniterParallel(t *testing.T) {
	want := make([][]byte, 0, len(bulk))
	for _, v := range bulk {
		data, err := json.Marshal(v)
		check(err)
		want = append(want, data)
	}

	got, err := MarshalParallel(15, bulk)
	check(err)

	for i := 0; i < len(bulk); i++ {
		assert.Equalf(t, want[i], got[i], "marshalled values different at index %d", i)
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func unmarshalFile(file string, v interface{}) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}
