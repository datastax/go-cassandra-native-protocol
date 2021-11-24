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
	"net"
	"testing"
)

var inet4 = Inet{
	Addr: inetAddr4,
	Port: 9042,
}

var inet4Bytes = append(inetAddr4Bytes,
	0, 0, 0x23, 0x52, //port
)

var inet6 = Inet{
	Addr: inetAddr6,
	Port: 9042,
}

var inet6Bytes = append(inetAddr6Bytes,
	0, 0, 0x23, 0x52, //port
)

func TestReadInet(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  *Inet
		remaining []byte
		err       error
	}{
		{"IPv4 INET", inet4Bytes[:], &inet4, []byte{}, nil},
		{"IPv6 INET", inet6Bytes[:], &inet6, []byte{}, nil},
		{"INET with remaining", append(inet4Bytes[:], 1, 2, 3, 4), &inet4, []byte{1, 2, 3, 4}, nil},
		{
			"cannot read INET length",
			[]byte{},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] address: %w", fmt.Errorf("cannot read [inetaddr] length: %w", fmt.Errorf("cannot read [byte]: %w", errors.New("EOF")))),
		},
		{
			"not enough bytes to read [inet] IPv4 content",
			[]byte{4, 192, 168, 1},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] address: %w", fmt.Errorf("cannot read [inetaddr] IPv4 content: %w", errors.New("unexpected EOF"))),
		},
		{
			"not enough bytes to read [inet] IPv6 content",
			[]byte{16, 0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] address: %w", fmt.Errorf("cannot read [inetaddr] IPv6 content: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read [inet] port number",
			[]byte{4, 192, 168, 1, 1, 0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] port number: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadInet(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteInet(t *testing.T) {
	tests := []struct {
		name     string
		input    *Inet
		expected []byte
		err      error
	}{
		{
			"IPv4 INET",
			&inet4,
			inet4Bytes,
			nil,
		},
		{
			"IPv6 INET",
			&inet6,
			inet6Bytes,
			nil,
		},
		{
			"INET with remaining",
			&inet4,
			inet4Bytes,
			nil,
		},
		{
			"cannot write nil INET",
			nil,
			nil,
			errors.New("cannot write nil [inet]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteInet(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfInet(t *testing.T) {
	tests := []struct {
		name     string
		input    *Inet
		expected int
		err      error
	}{
		{
			"IPv4 INET",
			&inet4,
			LengthOfByte + net.IPv4len + LengthOfInt,
			nil,
		},
		{
			"IPv6 INET",
			&inet6,
			LengthOfByte + net.IPv6len + LengthOfInt,
			nil,
		},
		{
			"nil INET",
			nil,
			-1,
			errors.New("cannot compute nil [inet] length"),
		},
		{
			"nil INET addr",
			&Inet{},
			-1,
			errors.New("cannot compute nil [inetaddr] length"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfInet(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}
