package primitives

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
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
			fmt.Errorf("not enough bytes to read [short bytes] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadShortBytes(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
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
