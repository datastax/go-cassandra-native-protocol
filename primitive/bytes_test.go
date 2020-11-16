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

func TestReadBytes(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"empty bytes", []byte{0, 0, 0, 0}, []byte{}, []byte{}, nil},
		{"nil bytes", []byte{0xff, 0xff, 0xff, 0xff}, nil, []byte{}, nil},
		{"singleton bytes", []byte{0, 0, 0, 1, 1}, []byte{1}, []byte{}, nil},
		{"simple bytes", []byte{0, 0, 0, 2, 1, 2}, []byte{1, 2}, []byte{}, nil},
		{
			"cannot read bytes length",
			[]byte{0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [bytes] length: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read bytes content",
			[]byte{0, 0, 0, 2, 1},
			nil,
			[]byte{},
			fmt.Errorf("not enough bytes to read [bytes] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadBytes(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
		err      error
	}{
		{
			"empty bytes",
			[]byte{},
			[]byte{0, 0, 0, 0},
			nil,
		},
		{
			"nil bytes",
			nil,
			[]byte{0xff, 0xff, 0xff, 0xff},
			nil,
		},
		{
			"singleton bytes",
			[]byte{1},
			[]byte{0, 0, 0, 1, 1},
			nil,
		},
		{
			"simple bytes",
			[]byte{1, 2},
			[]byte{0, 0, 0, 2, 1, 2},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteBytes(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
