package primitive

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

var inetAddr4 = net.IPv4(192, 168, 1, 1)

var inetAddr4Bytes = []byte{
	4,              // length of IP
	192, 168, 1, 1, // IP
}

// 2001:0db8:85a3:0000:0000:8a2e:0370:7334
var inetAddr6 = net.IP{0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34}

var inetAddr6Bytes = []byte{
	16,                                                                                             // length of IP
	0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34, // IP
}

func TestReadInetAddr(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  net.IP
		remaining []byte
		err       error
	}{
		{"IPv4 InetAddr", inetAddr4Bytes[:], inetAddr4, []byte{}, nil},
		{"IPv6 InetAddr", inetAddr6Bytes[:], inetAddr6, []byte{}, nil},
		{"InetAddr with remaining", append(inetAddr4Bytes[:], 1, 2, 3, 4), inetAddr4, []byte{1, 2, 3, 4}, nil},
		{
			"cannot read InetAddr length",
			[]byte{},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inetaddr] length: %w", fmt.Errorf("cannot read [byte]: %w", errors.New("EOF"))),
		},
		{
			"not enough bytes to read [inetaddr] IPv4 content",
			[]byte{4, 192, 168, 1},
			nil,
			[]byte{},
			errors.New("not enough bytes to read [inetaddr] IPv4 content"),
		},
		{
			"not enough bytes to read [inetaddr] IPv6 content",
			[]byte{16, 0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73},
			nil,
			[]byte{},
			errors.New("not enough bytes to read [inetaddr] IPv6 content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadInetAddr(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteInetAddr(t *testing.T) {
	tests := []struct {
		name     string
		input    net.IP
		expected []byte
		err      error
	}{
		{
			"IPv4 InetAddr",
			inetAddr4,
			inetAddr4Bytes,
			nil,
		},
		{
			"IPv6 InetAddr",
			inetAddr6,
			inetAddr6Bytes,
			nil,
		},
		{
			"cannot write nil InetAddr",
			nil,
			nil,
			errors.New("cannot write nil [inetaddr]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteInetAddr(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfInetAddr(t *testing.T) {
	tests := []struct {
		name     string
		input    net.IP
		expected int
		err      error
	}{
		{
			"IPv4 InetAddr",
			inetAddr4,
			LengthOfByte + net.IPv4len,
			nil,
		},
		{
			"IPv6 InetAddr",
			inetAddr6,
			LengthOfByte + net.IPv6len,
			nil,
		},
		{
			"nil InetAddr",
			nil,
			-1,
			errors.New("cannot compute nil [inetaddr] length"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfInetAddr(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}
