package primitives

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"testing"
)

func TestReadValue(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected *cassandraprotocol.Value
		err      error
	}{
		{
			"value length null",
			[]byte{
				0xff, 0xff, 0xff, 0xff, // length -1
			},
			cassandraprotocol.NewNullValue(),
			nil,
		},
		{
			"value length unset",
			[]byte{
				0xff, 0xff, 0xff, 0xfe, // length -2
			},
			cassandraprotocol.NewUnsetValue(),
			nil,
		},
		{
			"value empty",
			[]byte{
				0, 0, 0, 0, // length
			},
			cassandraprotocol.NewValue([]byte{}),
			nil,
		},
		{
			"value non empty",
			[]byte{
				0, 0, 0, 5, // length
				1, 2, 3, 4, 5, // contents
			},
			cassandraprotocol.NewValue([]byte{1, 2, 3, 4, 5}),
			nil,
		},
		{
			"cannot read value length",
			[]byte{
				0, 0, 0,
			},
			nil,
			fmt.Errorf("cannot read [value] length: %w",
				fmt.Errorf("cannot read [int]: %w",
					errors.New("unexpected EOF")),
			),
		},
		{
			"invalid value length",
			[]byte{
				0xff, 0xff, 0xff, 0xfd, // -3
			},
			nil,
			errors.New("invalid [value] length: -3"),
		},
		{
			"cannot read value contents",
			[]byte{
				0, 0, 0, 2, // length
				1, // contents
			},
			nil,
			errors.New("not enough bytes to read [value] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadValue(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteValue(t *testing.T) {
	tests := []struct {
		name     string
		input    *cassandraprotocol.Value
		expected []byte
		err      error
	}{
		{
			"nil value",
			nil,
			nil,
			errors.New("cannot write a nil [value]"),
		},
		{
			"empty value (type regular, nil contents)",
			&cassandraprotocol.Value{},
			[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
			nil,
		},
		{
			"non empty value (type regular, non nil contents)",
			&cassandraprotocol.Value{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{1, 2, 3, 4},
			},
			[]byte{0, 0, 0, 4, 1, 2, 3, 4},
			nil,
		},
		{
			"empty value with type null",
			&cassandraprotocol.Value{Type: cassandraprotocol.ValueTypeNull},
			[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
			nil,
		},
		{
			"empty value with type null but non nil contents",
			&cassandraprotocol.Value{
				Type:     cassandraprotocol.ValueTypeNull,
				Contents: []byte{1, 2, 3, 4},
			},
			[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
			nil,
		},
		{
			"empty value with type unset",
			&cassandraprotocol.Value{Type: cassandraprotocol.ValueTypeUnset},
			[]byte{0xff, 0xff, 0xff, 0xfe}, // length -2
			nil,
		},
		{
			"empty value with type unset but non nil contents",
			&cassandraprotocol.Value{
				Type:     cassandraprotocol.ValueTypeUnset,
				Contents: []byte{1, 2, 3, 4},
			},
			[]byte{0xff, 0xff, 0xff, 0xfe}, // length -2
			nil,
		},
		{
			"unknown type",
			&cassandraprotocol.Value{Type: 1},
			nil,
			errors.New("unknown [value] type: 1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteValue(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfValue(t *testing.T) {
	tests := []struct {
		name     string
		input    *cassandraprotocol.Value
		expected int
		err      error
	}{
		{
			"nil value",
			nil,
			-1,
			errors.New("cannot compute length of a nil [value]"),
		},
		{
			"empty value (type regular, nil contents)",
			&cassandraprotocol.Value{},
			LengthOfInt, // length -1
			nil,
		},
		{
			"non empty value (type regular, non nil contents)",
			cassandraprotocol.NewValue([]byte{1, 2, 3, 4}),
			LengthOfInt + len([]byte{1, 2, 3, 4}),
			nil,
		},
		{
			"empty value with type null",
			cassandraprotocol.NewNullValue(),
			LengthOfInt,
			nil,
		},
		{
			"empty value with type null but non nil contents",
			&cassandraprotocol.Value{
				Type:     cassandraprotocol.ValueTypeNull,
				Contents: []byte{1, 2, 3, 4},
			},
			LengthOfInt,
			nil,
		},
		{
			"empty value with type unset",
			cassandraprotocol.NewUnsetValue(),
			LengthOfInt,
			nil,
		},
		{
			"empty value with type unset but non nil contents",
			&cassandraprotocol.Value{
				Type:     cassandraprotocol.ValueTypeUnset,
				Contents: []byte{1, 2, 3, 4},
			},
			LengthOfInt,
			nil,
		},
		{
			"unknown type",
			&cassandraprotocol.Value{Type: 1},
			-1,
			errors.New("unknown [value] type: 1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfValue(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadPositionalValues(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected []*cassandraprotocol.Value
		err      error
	}{
		{
			"empty positional values",
			[]byte{0, 0},
			[]*cassandraprotocol.Value{},
			nil,
		},
		{
			"1 element, value empty",
			[]byte{
				0, 1, // length of list
				0, 0, 0, 0, // length of element
			},
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{},
			}},
			nil,
		},
		{
			"1 element, value non empty",
			[]byte{
				0, 1, // length of list
				0, 0, 0, 5, // length of element
				1, 2, 3, 4, 5, // contents of element
			},
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{1, 2, 3, 4, 5},
			}},
			nil,
		},
		{
			"3 elements",
			[]byte{
				0, 3, // length of list
				0xff, 0xff, 0xff, 0xff, // length of element 1
				0, 0, 0, 0, // length of element 2
				0, 0, 0, 5, // length of element 3
				1, 2, 3, 4, 5, // contents of element 3
			},
			[]*cassandraprotocol.Value{
				{
					Type:     cassandraprotocol.ValueTypeNull,
					Contents: nil,
				},
				{
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				},
				{
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				},
			},
			nil,
		},
		{
			"cannot read positional values length",
			[]byte{0},
			nil,
			fmt.Errorf("cannot read positional [value]s length: %w",
				fmt.Errorf("cannot read [short]: %w",
					errors.New("unexpected EOF"))),
		},
		{
			"cannot read positional values element",
			[]byte{0, 1, 0, 0, 0, 1},
			nil,
			fmt.Errorf("cannot read positional [value]s element 0 content: %w",
				fmt.Errorf("cannot read [value] content: %w",
					errors.New("EOF"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadPositionalValues(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWritePositionalValues(t *testing.T) {
	tests := []struct {
		name     string
		input    []*cassandraprotocol.Value
		expected []byte
		err      error
	}{
		{
			"nil positional values",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"nil positional values",
			[]*cassandraprotocol.Value{},
			[]byte{0, 0},
			nil,
		},
		{
			"1 element, value empty",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{},
			}},
			[]byte{
				0, 1, // length of list
				0, 0, 0, 0, // length of element
			},
			nil,
		},
		{
			"1 element, value null",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: nil,
			}},
			[]byte{
				0, 1, // length of list
				0xff, 0xff, 0xff, 0xff, // length of element
			},
			nil,
		},
		{
			"1 element, value null",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeNull,
				Contents: nil,
			}},
			[]byte{
				0, 1, // length of list
				0xff, 0xff, 0xff, 0xff, // length of element
			},
			nil,
		},
		{
			"1 element, value non empty",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{1, 2, 3, 4, 5},
			}},
			[]byte{
				0, 1, // length of list
				0, 0, 0, 5, // length of element
				1, 2, 3, 4, 5, // contents of element
			},
			nil,
		},
		{
			"4 elements",
			[]*cassandraprotocol.Value{
				{
					Type:     cassandraprotocol.ValueTypeNull,
					Contents: nil,
				},
				{
					Type:     cassandraprotocol.ValueTypeUnset,
					Contents: nil,
				},
				{
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				},
				{
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				},
			},
			[]byte{
				0, 4, // length of list
				0xff, 0xff, 0xff, 0xff, // length of element 1
				0xff, 0xff, 0xff, 0xfe, // length of element 2
				0, 0, 0, 0, // length of element 3
				0, 0, 0, 5, // length of element 4
				1, 2, 3, 4, 5, // contents of element 4
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WritePositionalValues(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfPositionalValues(t *testing.T) {
	tests := []struct {
		name     string
		input    []*cassandraprotocol.Value
		expected int
		err      error
	}{
		{
			"nil positional values",
			nil,
			LengthOfShort,
			nil,
		},
		{
			"nil positional values",
			[]*cassandraprotocol.Value{},
			LengthOfShort,
			nil,
		},
		{
			"1 element, value empty",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{},
			}},
			LengthOfShort + LengthOfInt,
			nil,
		},
		{
			"1 element, value null",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: nil,
			}},
			LengthOfShort + LengthOfInt,
			nil,
		},
		{
			"1 element, value null",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeNull,
				Contents: nil,
			}},
			LengthOfShort + LengthOfInt,
			nil,
		},
		{
			"1 element, value non empty",
			[]*cassandraprotocol.Value{{
				Type:     cassandraprotocol.ValueTypeRegular,
				Contents: []byte{1, 2, 3, 4, 5},
			}},
			LengthOfShort + LengthOfInt + len([]byte{1, 2, 3, 4, 5}),
			nil,
		},
		{
			"4 elements",
			[]*cassandraprotocol.Value{
				{
					Type:     cassandraprotocol.ValueTypeNull,
					Contents: nil,
				},
				{
					Type:     cassandraprotocol.ValueTypeUnset,
					Contents: nil,
				},
				{
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				},
				{
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				},
			},
			LengthOfShort + // length of list
				LengthOfInt + // length of element 1
				LengthOfInt + // length of element 2
				LengthOfInt + // length of element 3
				LengthOfInt + // length of element 4
				len([]byte{1, 2, 3, 4, 5}), // contents of element 4
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfPositionalValues(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadNamedValues(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected map[string]*cassandraprotocol.Value
		err      error
	}{
		{
			"empty named values",
			[]byte{0, 0},
			map[string]*cassandraprotocol.Value{},
			nil,
		},
		{
			"1 element, value empty",
			[]byte{
				0, 1, // length of list
				0, 0, // length of element key
				0, 0, 0, 0, // length of element
			},
			map[string]*cassandraprotocol.Value{
				"": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				}},
			nil,
		},
		{
			"1 element, value non empty",
			[]byte{
				0, 1, // length of list
				0, 5, // length of element key
				h, e, l, l, o, // contents of element key
				0, 0, 0, 5, // length of element value
				1, 2, 3, 4, 5, // contents of element value
			},
			map[string]*cassandraprotocol.Value{
				"hello": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				}},
			nil,
		},
		{
			"3 elements",
			[]byte{
				0, 3, // length of list
				0, 5, // length of element 1 key
				h, e, l, l, o, // contents of element 1 key
				0xff, 0xff, 0xff, 0xff, // length of element 1 value
				0, 5, // length of element 2 key
				w, o, r, l, d, // contents of element 2 key
				0, 0, 0, 0, // length of element 2 value
				0, 6, // length of element 2 key
				h, o, l, 0xc3, 0xa0, 0x21, // contents of element 2 key: holà!
				0, 0, 0, 5, // length of element 3 value
				1, 2, 3, 4, 5, // contents of element 3 value
			},
			map[string]*cassandraprotocol.Value{
				"hello": {
					Type:     cassandraprotocol.ValueTypeNull,
					Contents: nil,
				},
				"world": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				},
				"holà!": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				},
			},
			nil,
		},
		{
			"cannot read named values length",
			[]byte{0},
			nil,
			fmt.Errorf("cannot read named [value]s length: %w",
				fmt.Errorf("cannot read [short]: %w",
					errors.New("unexpected EOF"))),
		},
		{
			"cannot read named values element key",
			[]byte{0, 1, 0, 1},
			nil,
			fmt.Errorf("cannot read named [value]s entry 0 name: %w",
				fmt.Errorf("cannot read [string] content: %w",
					errors.New("EOF"))),
		},
		{
			"cannot read named values element value",
			[]byte{0, 1, 0, 1, h, 0, 1},
			nil,
			fmt.Errorf("cannot read named [value]s entry 0 content: %w",
				fmt.Errorf("cannot read [value] length: %w",
					fmt.Errorf("cannot read [int]: %w",
						errors.New("unexpected EOF")))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadNamedValues(buf)
			assert.EqualValues(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteNamedValues(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]*cassandraprotocol.Value
		expected []byte
		err      error
	}{
		{
			"nil named values",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"nil named values",
			map[string]*cassandraprotocol.Value{},
			[]byte{0, 0},
			nil,
		},
		{
			"1 element, value empty",
			map[string]*cassandraprotocol.Value{
				"": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				}},
			[]byte{
				0, 1, // length of list
				0, 0, // length of element key
				0, 0, 0, 0, // length of element
			},
			nil,
		},
		{
			"1 element, value non empty",
			map[string]*cassandraprotocol.Value{
				"hello": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				}},
			[]byte{
				0, 1, // length of list
				0, 5, // length of element key
				h, e, l, l, o, // contents of element key
				0, 0, 0, 5, // length of element value
				1, 2, 3, 4, 5, // contents of element value
			},
			nil,
		},
		{
			"3 elements",
			map[string]*cassandraprotocol.Value{
				"hello": {
					Type:     cassandraprotocol.ValueTypeNull,
					Contents: nil,
				},
				"world": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{},
				},
				"holà!": {
					Type:     cassandraprotocol.ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				},
			},
			[]byte{
				0, 3, // length of list
				0, 5, // length of element 1 key
				h, e, l, l, o, // contents of element 1 key
				0xff, 0xff, 0xff, 0xff, // length of element 1 value
				0, 5, // length of element 2 key
				w, o, r, l, d, // contents of element 2 key
				0, 0, 0, 0, // length of element 2 value
				0, 6, // length of element 2 key
				h, o, l, 0xc3, 0xa0, 0x21, // contents of element 2 key: holà!
				0, 0, 0, 5, // length of element 3 value
				1, 2, 3, 4, 5, // contents of element 3 value
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteNamedValues(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

//func TestLengthOfNamedValues(t *testing.T) {
//	tests := []struct {
//		name     string
//		input    map[string]*cassandraprotocol.Value
//		expected int
//		err      error
//	}{
//		{
//			"nil named values",
//			nil,
//			LengthOfShort,
//			nil,
//		},
//		{
//			"nil named values",
//			map[string]*cassandraprotocol.Value{},
//			LengthOfShort,
//			nil,
//		},
//		{
//			"1 element, value empty",
//			map[string]*cassandraprotocol.Value{{
//				Type:     cassandraprotocol.ValueTypeRegular,
//				Contents: []byte{},
//			}},
//			LengthOfShort + LengthOfInt,
//			nil,
//		},
//		{
//			"1 element, value null",
//			map[string]*cassandraprotocol.Value{{
//				Type:     cassandraprotocol.ValueTypeRegular,
//				Contents: nil,
//			}},
//			LengthOfShort + LengthOfInt,
//			nil,
//		},
//		{
//			"1 element, value null",
//			map[string]*cassandraprotocol.Value{{
//				Type:     cassandraprotocol.ValueTypeNull,
//				Contents: nil,
//			}},
//			LengthOfShort + LengthOfInt,
//			nil,
//		},
//		{
//			"1 element, value non empty",
//			map[string]*cassandraprotocol.Value{{
//				Type:     cassandraprotocol.ValueTypeRegular,
//				Contents: []byte{1, 2, 3, 4, 5},
//			}},
//			LengthOfShort + LengthOfInt + len([]byte{1, 2, 3, 4, 5}),
//			nil,
//		},
//		{
//			"4 elements",
//			map[string]*cassandraprotocol.Value{
//				{
//					Type:     cassandraprotocol.ValueTypeNull,
//					Contents: nil,
//				},
//				{
//					Type:     cassandraprotocol.ValueTypeUnset,
//					Contents: nil,
//				},
//				{
//					Type:     cassandraprotocol.ValueTypeRegular,
//					Contents: []byte{},
//				},
//				{
//					Type:     cassandraprotocol.ValueTypeRegular,
//					Contents: []byte{1, 2, 3, 4, 5},
//				},
//			},
//			LengthOfShort + // length of list
//				LengthOfInt + // length of element 1
//				LengthOfInt + // length of element 2
//				LengthOfInt + // length of element 3
//				LengthOfInt + // length of element 4
//				len([]byte{1, 2, 3, 4, 5}), // contents of element 4
//			nil,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			actual, err := LengthOfNamedValues(tt.input)
//			assert.Equal(t, tt.expected, actual)
//			assert.Equal(t, tt.err, err)
//		})
//	}
//}
