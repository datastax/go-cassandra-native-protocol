package primitive

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	d = byte('d')
	e = byte('e')
	h = byte('h')
	k = byte('k')
	l = byte('l')
	m = byte('m')
	n = byte('n')
	o = byte('o')
	r = byte('r')
	u = byte('u')
	w = byte('w')
)

func TestReadString(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  string
		remaining []byte
		err       error
	}{
		{"simple string", []byte{0, 5, h, e, l, l, o}, "hello", []byte{}, nil},
		{"string with remaining", []byte{0, 5, h, e, l, l, o, 1, 2, 3, 4}, "hello", []byte{1, 2, 3, 4}, nil},
		{"empty string", []byte{0, 0}, "", []byte{}, nil},
		{"non-ASCII string", []byte{
			0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, "γειά σου", []byte{}, nil},
		{
			"cannot read length",
			[]byte{0},
			"",
			[]byte{},
			fmt.Errorf("cannot read [string] length: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read string",
			[]byte{0, 5, h, e, l, l},
			"",
			[]byte{},
			errors.New("not enough bytes to read [string] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadString(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		err      error
	}{
		{
			"simple string",
			"hello",
			[]byte{0, 5, h, e, l, l, o},
			nil,
		},
		{"empty string", "", []byte{0, 0}, nil},
		{"non-ASCII string", "γειά σου", []byte{
			0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteString(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
