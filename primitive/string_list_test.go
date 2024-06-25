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

func TestReadStringList(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  []string
		remaining []byte
		err       error
	}{
		{"empty string list", []byte{0, 0}, []string{}, []byte{}, nil},
		{"singleton string list", []byte{
			0, 1, // length
			0, 5, h, e, l, l, o, // hello
		}, []string{"hello"}, []byte{}, nil},
		{"simple string list", []byte{
			0, 2, // length
			0, 5, h, e, l, l, o, // hello
			0, 5, w, o, r, l, d, // world
		}, []string{"hello", "world"}, []byte{}, nil},
		{"empty elements", []byte{
			0, 2, // length
			0, 0, // elt 1
			0, 0, // elt 2
		}, []string{"", ""}, []byte{}, nil},
		{
			"cannot read list length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string list] length: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read list element",
			[]byte{0, 1, 0, 5, h, e, l, l},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string list] element 0: %w", fmt.Errorf("cannot read [string] content: %w", errors.New("unexpected EOF"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.source)
			actual, err := ReadStringList(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
			remaining, _ := ioutil.ReadAll(buf)
			assert.Equal(t, tt.remaining, remaining)
		})
	}
}

func TestWriteStringList(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []byte
		err      error
	}{
		{
			"empty string list",
			[]string{},
			[]byte{0, 0},
			nil,
		},
		{
			"nil string list",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"singleton string list",
			[]string{"hello"},
			[]byte{
				0, 1, // length
				0, 5, h, e, l, l, o, // hello
			},
			nil,
		},
		{
			"simple string list",
			[]string{"hello", "world"},
			[]byte{
				0, 2, // length
				0, 5, h, e, l, l, o, // hello
				0, 5, w, o, r, l, d, // world
			},
			nil,
		},
		{
			"empty elements",
			[]string{"", ""},
			[]byte{
				0, 2, // length
				0, 0, // elt 1
				0, 0, // elt 2
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteStringList(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
