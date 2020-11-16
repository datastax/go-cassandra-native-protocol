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
	"testing"
)

func TestReadStringMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string]string
		remaining []byte
		err       error
	}{
		{"empty string map", []byte{0, 0}, map[string]string{}, []byte{}, nil},
		{"map 1 key", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 5, w, o, r, l, d, // value1: world
		}, map[string]string{"hello": "world"}, []byte{}, nil},
		// FIXME map iteration order
		//{"map 2 keys", []byte{
		//	0, 2, // map length
		//	0, 5, h, e, l, l, o, // key1: hello
		//	0, 5, w, o, r, l, d, // value1: world
		//	0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//	0, 5, m, u, n, d, o, // value2: mundo
		//}, map[string]string{
		//	"hello": "world",
		//	"holà!": "mundo",
		//}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] length: %w",
				fmt.Errorf("cannot read [short]: %w",
					errors.New("unexpected EOF"))),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] entry 0 key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w",
						errors.New("unexpected EOF")))),
		},
		{
			"cannot read key",
			[]byte{0, 1, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] entry 0 key: %w",
				errors.New("not enough bytes to read [string] content")),
		},
		{
			"cannot read value length",
			[]byte{0, 1, 0, 1, k, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] entry 0 value: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")))),
		},
		{
			"cannot read value",
			[]byte{0, 1, 0, 1, k, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string map] entry 0 value: %w",
				errors.New("not enough bytes to read [string] content"),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadStringMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteStringMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected []byte
		err      error
	}{
		{
			"empty string map",
			map[string]string{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil string map",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"map 1 key",
			map[string]string{"hello": "world"},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		// FIXME map iteration order
		//{"map 2 keys",
		//	map[string]string{
		//		"hello": "world",
		//		"holà!": "mundo",
		//	},
		//	[]byte{
		//		0, 2, // map length
		//		0, 5, h, e, l, l, o, // key1: hello
		//		0, 5, w, o, r, l, d, // value1: world
		//		0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//		0, 5, m, u, n, d, o, // value2: mundo
		//	}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteStringMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
