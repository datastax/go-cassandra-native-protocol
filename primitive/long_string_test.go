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

func TestReadLongString(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  string
		remaining []byte
		err       error
	}{
		{"simple string", []byte{0, 0, 0, 5, h, e, l, l, o}, "hello", []byte{}, nil},
		{"string with remaining", []byte{0, 0, 0, 5, h, e, l, l, o, 1, 2, 3, 4}, "hello", []byte{1, 2, 3, 4}, nil},
		{"empty string", []byte{0, 0, 0, 0}, "", []byte{}, nil},
		{"non-ASCII string", []byte{
			0, 0, 0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, "γειά σου", []byte{}, nil},
		{
			"cannot read length",
			[]byte{0, 0, 0},
			"",
			[]byte{},
			fmt.Errorf("cannot read [long string] length: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read string",
			[]byte{0, 0, 0, 5, h, e, l, l},
			"",
			[]byte{},
			fmt.Errorf("cannot read [long string] content: %w", errors.New("unexpected EOF")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.source)
			actual, err := ReadLongString(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
			remaining, _ := ioutil.ReadAll(buf)
			assert.Equal(t, tt.remaining, remaining)
		})
	}
}

func TestWriteLongString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		err      error
	}{
		{
			"simple string",
			"hello",
			[]byte{0, 0, 0, 5, h, e, l, l, o},
			nil,
		},
		{
			"empty string",
			"",
			[]byte{0, 0, 0, 0},
			nil,
		},
		{
			"non-ASCII string",
			"γειά σου",
			[]byte{
				0, 0, 0, 15, // length
				0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
				0x20,                               // space
				0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteLongString(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
