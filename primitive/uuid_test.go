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

var uuid = UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
var uuidBytes = [16]byte{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}

func TestReadUuid(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  *UUID
		remaining []byte
		err       error
	}{
		{"simple UUID", uuidBytes[:], &uuid, []byte{}, nil},
		{"UUID with remaining", append(uuidBytes[:], 1, 2, 3, 4), &uuid, []byte{1, 2, 3, 4}, nil},
		{
			"cannot read UUID",
			uuidBytes[:15],
			nil,
			[]byte{},
			fmt.Errorf("cannot read [uuid] content: %w", errors.New("unexpected EOF")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadUuid(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteUuid(t *testing.T) {
	tests := []struct {
		name     string
		input    *UUID
		expected []byte
		err      error
	}{
		{
			"simple UUID",
			&uuid,
			uuidBytes[:],
			nil,
		},
		{
			"UUID with remaining",
			&uuid,
			uuidBytes[:],
			nil,
		},
		{
			"nil UUID",
			nil,
			nil,
			errors.New("cannot write nil [uuid]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteUuid(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestUUID_Clone(t *testing.T) {
	u := &UUID{0, 1, 2, 3, 4, 5, 6}
	cloned := u.Clone()

	assert.Equal(t, u, cloned)

	cloned[1] = 9
	assert.NotEqual(t, u, cloned)
}
