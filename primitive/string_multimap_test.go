// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package primitive

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestReadStringMultiMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string][]string
		remaining []byte
		err       error
	}{
		{"empty string multimap", []byte{0, 0}, map[string][]string{}, []byte{}, nil},
		{"multimap 1 key 1 value", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 1, // list length
			0, 5, w, o, r, l, d, // value1: world
		}, map[string][]string{"hello": {"world"}}, []byte{}, nil},
		{"multimap 1 key 2 values", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 2, // list length
			0, 5, w, o, r, l, d, // value1: world
			0, 5, m, u, n, d, o, // value2: mundo
		}, map[string][]string{"hello": {"world", "mundo"}}, []byte{}, nil},
		// FIXME map iteration order
		//{"multimap 2 keys 2 values", []byte{
		//	0, 2, // map length
		//	0, 5, h, e, l, l, o, // key1: hello
		//	0, 2, // list length
		//	0, 5, w, o, r, l, d, // value1: world
		//	0, 5, m, u, n, d, o, // value2: mundo
		//	0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//	0, 2, // list length
		//	0, 5, w, o, r, l, d, // value1: world
		//	0, 5, m, u, n, d, o, // value2: mundo
		//}, map[string][]string{
		//	"hello": {"world", "mundo"},
		//	"holà!": {"world", "mundo"},
		//}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] length: %w",
				fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")),
			),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] entry 0 key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read list length",
			[]byte{0, 1, 0, 1, k, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] entry 0 value: %w",
				fmt.Errorf("cannot read [string list] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read element length",
			[]byte{0, 1, 0, 1, k, 0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] entry 0 value: %w",
				fmt.Errorf("cannot read [string list] element 0: %w",
					fmt.Errorf("cannot read [string] length: %w",
						fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")))),
			),
		},
		{
			"cannot read list",
			[]byte{0, 1, 0, 1, k, 0, 1, 0, 5, h, e, l, l},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string multimap] entry 0 value: %w",
				fmt.Errorf("cannot read [string list] element 0: %w",
					fmt.Errorf("cannot read [string] content: %w", errors.New("unexpected EOF")))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.source)
			actual, err := ReadStringMultiMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
			remaining, _ := ioutil.ReadAll(buf)
			assert.Equal(t, tt.remaining, remaining)
		})
	}
}

func TestWriteStringMultiMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]string
		expected []byte
		err      error
	}{
		{
			"empty string multimap",
			map[string][]string{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil string multimap",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"multimap 1 key 1 value",
			map[string][]string{"hello": {"world"}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 1, // list length
				0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		{
			"multimap 1 key 2 values",
			map[string][]string{"hello": {"world", "mundo"}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 2, // list length
				0, 5, w, o, r, l, d, // value1: world
				0, 5, m, u, n, d, o, // value2: mundo
			},
			nil,
		},
		// FIXME map iteration order
		//{"multimap 2 keys 2 values",
		//	map[string][]string{
		//		"hello": {"world", "mundo"},
		//		"holà!": {"world", "mundo"},
		//	},
		//	[]byte{
		//		0, 2, // map length
		//		0, 5, h, e, l, l, o, // key1: hello
		//		0, 2, // list length
		//		0, 5, w, o, r, l, d, // value1: world
		//		0, 5, m, u, n, d, o, // value2: mundo
		//		0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//		0, 2, // list length
		//		0, 5, w, o, r, l, d, // value1: world
		//		0, 5, m, u, n, d, o, // value2: mundo
		//	},
		//	nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteStringMultiMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
