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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadReasonMap(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected []*FailureReason
		err      error
	}{
		{
			"reason nmap empty",
			[]byte{
				0, 0, 0, 0, // length
			},
			[]*FailureReason{},
			nil,
		},
		{
			"reason map 1 key",
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1, 1, // key
				0, 1, // value
			},
			[]*FailureReason{{net.IPv4(192, 168, 1, 1), FailureCodeTooManyTombstonesRead}},
			nil,
		},
		{
			"cannot read reason map length",
			[]byte{
				0, 0, 0,
			},
			nil,
			fmt.Errorf("cannot read reason map length: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read reason map key",
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1,
			},
			nil,
			fmt.Errorf("cannot read reason map key for element 0: %w", fmt.Errorf("cannot read [inetaddr] IPv4 content: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read reason map value",
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1, 1, // key
				0, // incomplete value
			},
			nil,
			fmt.Errorf("cannot read reason map value for element 0: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadReasonMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteReasonMap(t *testing.T) {
	tests := []struct {
		name     string
		input    []*FailureReason
		expected []byte
		err      error
	}{
		{
			"empty string map",
			[]*FailureReason{},
			[]byte{0, 0, 0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil string map",
			nil,
			[]byte{0, 0, 0, 0},
			nil,
		},
		{
			"map 1 key",
			[]*FailureReason{{net.IPv4(192, 168, 1, 1), FailureCodeTooManyTombstonesRead}},
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1, 1, // key
				0, 1, // value
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteReasonMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfReasonMap(t *testing.T) {
	var inetAddr4Length, _ = LengthOfInetAddr(inetAddr4)
	tests := []struct {
		name     string
		input    []*FailureReason
		expected int
		err      error
	}{
		{
			"empty string map",
			[]*FailureReason{},
			LengthOfInt,
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil string map",
			nil,
			LengthOfInt,
			nil,
		},
		{
			"map 1 key",
			[]*FailureReason{{net.IPv4(192, 168, 1, 1), 42}},
			LengthOfInt + inetAddr4Length + LengthOfShort,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfReasonMap(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}
