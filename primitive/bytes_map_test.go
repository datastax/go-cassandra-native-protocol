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
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadBytesMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string][]byte
		remaining []byte
		err       error
	}{
		{"empty bytes map", []byte{0, 0}, map[string][]byte{}, []byte{}, nil},
		{"map 1 key", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 0, 0, 5, w, o, r, l, d, // value1: world
		}, map[string][]byte{"hello": {w, o, r, l, d}}, []byte{}, nil},
		// FIXME map iteration order
		//{"map 2 keys", []byte{
		//	0, 2, // map length
		//	0, 5, h, e, l, l, o, // key1: hello
		//	0, 0, 0, 5, w, o, r, l, d, // value1: world
		//	0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//	0, 0, 0, 5, m, u, n, d, o, // value2: mundo
		//}, map[string][]byte{
		//	"hello": {w, o, r, l, d},
		//	"holà!": {m, u, n, d, o},
		//}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] length: %w",
				fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")),
			),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read key",
			[]byte{0, 1, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 key: %w",
				fmt.Errorf("cannot read [string] content: %w", errors.New("unexpected EOF")),
			),
		},
		{
			"cannot read value length",
			[]byte{0, 1, 0, 1, k, 0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 value: %w",
				fmt.Errorf("cannot read [bytes] length: %w",
					fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read value",
			[]byte{0, 1, 0, 1, k, 0, 0, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 value: %w",
				fmt.Errorf("cannot read [bytes] content: %w", errors.New("unexpected EOF")),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.source)
			actual, err := ReadBytesMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
			remaining, _ := ioutil.ReadAll(buf)
			assert.Equal(t, tt.remaining, remaining)
		})
	}
}

func TestWriteBytesMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]byte
		expected []byte
		err      error
	}{
		{
			"empty bytes map",
			map[string][]byte{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil bytes map",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"map 1 key",
			map[string][]byte{"hello": {w, o, r, l, d}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 0, 0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		// FIXME map iteration order
		//{"map 2 keys",
		//	map[string][]byte{
		//		"hello": {w, o, r, l, d},
		//		"holà!": {m, u, n, d, o},
		//	},
		//	[]byte{
		//		0, 2, // map length
		//		0, 5, h, e, l, l, o, // key1: hello
		//		0, 0, 0, 5, w, o, r, l, d, // value1: world
		//		0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//		0, 0, 0, 5, m, u, n, d, o, // value2: mundo
		//	}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteBytesMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
