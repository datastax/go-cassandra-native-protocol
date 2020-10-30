package primitive

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadBytesMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string][]byte
		remaining []byte
		err       error
	}{
		{"empty bytes map", []byte{0, 0}, map[string][]byte{}, []byte{}, nil},
		{"map 1 key", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 0, 0, 5, w, o, r, l, d, // value1: world
		}, map[string][]byte{"hello": {w, o, r, l, d}}, []byte{}, nil},
		// FIXME map iteration order
		//{"map 2 keys", []byte{
		//	0, 2, // map length
		//	0, 5, h, e, l, l, o, // key1: hello
		//	0, 0, 0, 5, w, o, r, l, d, // value1: world
		//	0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//	0, 0, 0, 5, m, u, n, d, o, // value2: mundo
		//}, map[string][]byte{
		//	"hello": {w, o, r, l, d},
		//	"holà!": {m, u, n, d, o},
		//}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] length: %w",
				fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")),
			),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read key",
			[]byte{0, 1, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 key: %w",
				errors.New("not enough bytes to read [string] content"),
			),
		},
		{
			"cannot read value length",
			[]byte{0, 1, 0, 1, k, 0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 value: %w",
				fmt.Errorf("cannot read [bytes] length: %w",
					fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read value",
			[]byte{0, 1, 0, 1, k, 0, 0, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] entry 0 value: %w",
				errors.New("not enough bytes to read [bytes] content"),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadBytesMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteBytesMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]byte
		expected []byte
		err      error
	}{
		{
			"empty bytes map",
			map[string][]byte{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil bytes map",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"map 1 key",
			map[string][]byte{"hello": {w, o, r, l, d}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 0, 0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		// FIXME map iteration order
		//{"map 2 keys",
		//	map[string][]byte{
		//		"hello": {w, o, r, l, d},
		//		"holà!": {m, u, n, d, o},
		//	},
		//	[]byte{
		//		0, 2, // map length
		//		0, 5, h, e, l, l, o, // key1: hello
		//		0, 0, 0, 5, w, o, r, l, d, // value1: world
		//		0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
		//		0, 0, 0, 5, m, u, n, d, o, // value2: mundo
		//	}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteBytesMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
