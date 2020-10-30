package primitive

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadReasonMap(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected map[string]uint16
		err      error
	}{
		{
			"reason nmap empty",
			[]byte{
				0, 0, 0, 0, // length
			},
			map[string]uint16{},
			nil,
		},
		{
			"reason map 1 key",
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1, 1, // key
				0, 42, // value
			},
			map[string]uint16{"192.168.1.1": 42},
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
			fmt.Errorf("cannot read reason map key: %w", fmt.Errorf("not enough bytes to read [inetaddr] IPv4 content")),
		},
		{
			"cannot read reason map value",
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1, 1, // key
				0, // incomplete value
			},
			nil,
			fmt.Errorf("cannot read reason map value: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
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
		input    map[string]uint16
		expected []byte
		err      error
	}{
		{
			"empty string map",
			map[string]uint16{},
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
			map[string]uint16{"192.168.1.1": 42},
			[]byte{
				0, 0, 0, 1, // length
				4, 192, 168, 1, 1, // key
				0, 42, // value
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
		input    map[string]uint16
		expected int
		err      error
	}{
		{
			"empty string map",
			map[string]uint16{},
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
			map[string]uint16{"192.168.1.1": 42},
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
