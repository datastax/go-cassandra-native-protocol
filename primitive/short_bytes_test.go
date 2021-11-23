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

func TestReadShortBytes(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"empty short bytes", []byte{0, 0}, []byte{}, []byte{}, nil},
		{"singleton short bytes", []byte{0, 1, 1}, []byte{1}, []byte{}, nil},
		{"simple short bytes", []byte{0, 2, 1, 2}, []byte{1, 2}, []byte{}, nil},
		{
			"cannot read short bytes length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [short bytes] length: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read short bytes content",
			[]byte{0, 2, 1},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [short bytes] content: %w", errors.New("unexpected EOF")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.source)
			actual, err := ReadShortBytes(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
			remaining, _ := ioutil.ReadAll(buf)
			assert.Equal(t, tt.remaining, remaining)
		})
	}
}

func TestWriteShortBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
		err      error
	}{
		{
			"empty short bytes",
			[]byte{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil short bytes",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"singleton short bytes",
			[]byte{1},
			[]byte{0, 1, 1},
			nil,
		},
		{
			"simple short bytes",
			[]byte{1, 2},
			[]byte{0, 2, 1, 2},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteShortBytes(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
