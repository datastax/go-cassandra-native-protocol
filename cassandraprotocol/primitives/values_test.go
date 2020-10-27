package primitives

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadValue(t *testing.T) {
	// versions < 4
	for _, version := range cassandraprotocol.AllProtocolVersionsLesserThan(cassandraprotocol.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				expected *Value
				err      error
			}{
				{
					"value length null",
					[]byte{
						0xff, 0xff, 0xff, 0xff, // length -1
					},
					NewNullValue(),
					nil,
				},
				{
					"value length unset",
					[]byte{
						0xff, 0xff, 0xff, 0xfe, // length -2
					},
					nil,
					fmt.Errorf("cannot use unset value in protocol version: 3"),
				},
				{
					"value empty",
					[]byte{
						0, 0, 0, 0, // length
					},
					NewValue([]byte{}),
					nil,
				},
				{
					"value non empty",
					[]byte{
						0, 0, 0, 5, // length
						1, 2, 3, 4, 5, // contents
					},
					NewValue([]byte{1, 2, 3, 4, 5}),
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
					actual, err := ReadValue(buf, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// versions >= 4
	for _, version := range cassandraprotocol.AllProtocolVersionsGreaterThanOrEqualTo(cassandraprotocol.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			var tests = []struct {
				name     string
				source   []byte
				expected *Value
				err      error
			}{
				{
					"value length null",
					[]byte{
						0xff, 0xff, 0xff, 0xff, // length -1
					},
					NewNullValue(),
					nil,
				},
				{
					"value length unset",
					[]byte{
						0xff, 0xff, 0xff, 0xfe, // length -2
					},
					NewUnsetValue(),
					nil,
				},
				{
					"value empty",
					[]byte{
						0, 0, 0, 0, // length
					},
					NewValue([]byte{}),
					nil,
				},
				{
					"value non empty",
					[]byte{
						0, 0, 0, 5, // length
						1, 2, 3, 4, 5, // contents
					},
					NewValue([]byte{1, 2, 3, 4, 5}),
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
					actual, err := ReadValue(buf, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestWriteValue(t *testing.T) {
	// versions < 4
	for _, version := range []cassandraprotocol.ProtocolVersion{cassandraprotocol.ProtocolVersion3} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    *Value
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
					&Value{},
					[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
					nil,
				},
				{
					"non empty value (type regular, non nil contents)",
					&Value{
						Type:     ValueTypeRegular,
						Contents: []byte{1, 2, 3, 4},
					},
					[]byte{0, 0, 0, 4, 1, 2, 3, 4},
					nil,
				},
				{
					"empty value with type null",
					&Value{Type: ValueTypeNull},
					[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
					nil,
				},
				{
					"empty value with type null but non nil contents",
					&Value{
						Type:     ValueTypeNull,
						Contents: []byte{1, 2, 3, 4},
					},
					[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
					nil,
				},
				{
					"empty value with type unset",
					&Value{Type: ValueTypeUnset},
					nil,
					errors.New("cannot use unset value in protocol version: 3"),
				},
				{
					"empty value with type unset but non nil contents",
					&Value{
						Type:     ValueTypeUnset,
						Contents: []byte{1, 2, 3, 4},
					},
					nil,
					errors.New("cannot use unset value in protocol version: 3"),
				},
				{
					"unknown type",
					&Value{Type: 1},
					nil,
					errors.New("unknown [value] type: 1"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := &bytes.Buffer{}
					err := WriteValue(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// versions >= 4
	for _, version := range cassandraprotocol.AllProtocolVersionsGreaterThanOrEqualTo(cassandraprotocol.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			var tests = []struct {
				name     string
				input    *Value
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
					&Value{},
					[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
					nil,
				},
				{
					"non empty value (type regular, non nil contents)",
					&Value{
						Type:     ValueTypeRegular,
						Contents: []byte{1, 2, 3, 4},
					},
					[]byte{0, 0, 0, 4, 1, 2, 3, 4},
					nil,
				},
				{
					"empty value with type null",
					&Value{Type: ValueTypeNull},
					[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
					nil,
				},
				{
					"empty value with type null but non nil contents",
					&Value{
						Type:     ValueTypeNull,
						Contents: []byte{1, 2, 3, 4},
					},
					[]byte{0xff, 0xff, 0xff, 0xff}, // length -1
					nil,
				},
				{
					"empty value with type unset",
					&Value{Type: ValueTypeUnset},
					[]byte{0xff, 0xff, 0xff, 0xfe}, // length -2
					nil,
				},
				{
					"empty value with type unset but non nil contents",
					&Value{
						Type:     ValueTypeUnset,
						Contents: []byte{1, 2, 3, 4},
					},
					[]byte{0xff, 0xff, 0xff, 0xfe}, // length -2
					nil,
				},
				{
					"unknown type",
					&Value{Type: 1},
					nil,
					errors.New("unknown [value] type: 1"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := &bytes.Buffer{}
					err := WriteValue(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestLengthOfValue(t *testing.T) {
	tests := []struct {
		name     string
		input    *Value
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
			&Value{},
			LengthOfInt, // length -1
			nil,
		},
		{
			"non empty value (type regular, non nil contents)",
			NewValue([]byte{1, 2, 3, 4}),
			LengthOfInt + len([]byte{1, 2, 3, 4}),
			nil,
		},
		{
			"empty value with type null",
			NewNullValue(),
			LengthOfInt,
			nil,
		},
		{
			"empty value with type null but non nil contents",
			&Value{
				Type:     ValueTypeNull,
				Contents: []byte{1, 2, 3, 4},
			},
			LengthOfInt,
			nil,
		},
		{
			"empty value with type unset",
			NewUnsetValue(),
			LengthOfInt,
			nil,
		},
		{
			"empty value with type unset but non nil contents",
			&Value{
				Type:     ValueTypeUnset,
				Contents: []byte{1, 2, 3, 4},
			},
			LengthOfInt,
			nil,
		},
		{
			"unknown type",
			&Value{Type: 1},
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
	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				expected []*Value
				err      error
			}{
				{
					"empty positional values",
					[]byte{0, 0},
					[]*Value{},
					nil,
				},
				{
					"1 element, value empty",
					[]byte{
						0, 1, // length of list
						0, 0, 0, 0, // length of element
					},
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{
						{
							Type:     ValueTypeNull,
							Contents: nil,
						},
						{
							Type:     ValueTypeRegular,
							Contents: []byte{},
						},
						{
							Type:     ValueTypeRegular,
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
					actual, err := ReadPositionalValues(buf, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestWritePositionalValues(t *testing.T) {
	// versions < 4
	for _, version := range cassandraprotocol.AllProtocolVersionsLesserThan(cassandraprotocol.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []*Value
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
					[]*Value{},
					[]byte{0, 0},
					nil,
				},
				{
					"1 element, value empty",
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{{
						Type:     ValueTypeNull,
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
					[]*Value{{
						Type:     ValueTypeRegular,
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
					"3 elements",
					[]*Value{
						{
							Type:     ValueTypeNull,
							Contents: nil,
						},
						{
							Type:     ValueTypeRegular,
							Contents: []byte{},
						},
						{
							Type:     ValueTypeRegular,
							Contents: []byte{1, 2, 3, 4, 5},
						},
					},
					[]byte{
						0, 3, // length of list
						0xff, 0xff, 0xff, 0xff, // length of element 1
						0, 0, 0, 0, // length of element 2
						0, 0, 0, 5, // length of element 3
						1, 2, 3, 4, 5, // contents of element 3
					},
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := &bytes.Buffer{}
					err := WritePositionalValues(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// versions >= 4
	for _, version := range cassandraprotocol.AllProtocolVersionsGreaterThanOrEqualTo(cassandraprotocol.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			var tests = []struct {
				name     string
				input    []*Value
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
					[]*Value{},
					[]byte{0, 0},
					nil,
				},
				{
					"1 element, value empty",
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{{
						Type:     ValueTypeNull,
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
					[]*Value{{
						Type:     ValueTypeRegular,
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
					[]*Value{
						{
							Type:     ValueTypeNull,
							Contents: nil,
						},
						{
							Type:     ValueTypeUnset,
							Contents: nil,
						},
						{
							Type:     ValueTypeRegular,
							Contents: []byte{},
						},
						{
							Type:     ValueTypeRegular,
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
					err := WritePositionalValues(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestLengthOfPositionalValues(t *testing.T) {
	tests := []struct {
		name     string
		input    []*Value
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
			[]*Value{},
			LengthOfShort,
			nil,
		},
		{
			"1 element, value empty",
			[]*Value{{
				Type:     ValueTypeRegular,
				Contents: []byte{},
			}},
			LengthOfShort + LengthOfInt,
			nil,
		},
		{
			"1 element, value null",
			[]*Value{{
				Type:     ValueTypeRegular,
				Contents: nil,
			}},
			LengthOfShort + LengthOfInt,
			nil,
		},
		{
			"1 element, value null",
			[]*Value{{
				Type:     ValueTypeNull,
				Contents: nil,
			}},
			LengthOfShort + LengthOfInt,
			nil,
		},
		{
			"1 element, value non empty",
			[]*Value{{
				Type:     ValueTypeRegular,
				Contents: []byte{1, 2, 3, 4, 5},
			}},
			LengthOfShort + LengthOfInt + len([]byte{1, 2, 3, 4, 5}),
			nil,
		},
		{
			"4 elements",
			[]*Value{
				{
					Type:     ValueTypeNull,
					Contents: nil,
				},
				{
					Type:     ValueTypeUnset,
					Contents: nil,
				},
				{
					Type:     ValueTypeRegular,
					Contents: []byte{},
				},
				{
					Type:     ValueTypeRegular,
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
	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				expected map[string]*Value
				err      error
			}{
				{
					"empty named values",
					[]byte{0, 0},
					map[string]*Value{},
					nil,
				},
				{
					"1 element, value empty",
					[]byte{
						0, 1, // length of list
						0, 0, // length of element key
						0, 0, 0, 0, // length of element
					},
					map[string]*Value{
						"": {
							Type:     ValueTypeRegular,
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
					map[string]*Value{
						"hello": {
							Type:     ValueTypeRegular,
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
						0, 6, // length of element 3 key
						h, o, l, 0xc3, 0xa0, 0x21, // contents of element 3 key: holà!
						0, 0, 0, 5, // length of element 3 value
						1, 2, 3, 4, 5, // contents of element 3 value
					},
					map[string]*Value{
						"hello": {
							Type:     ValueTypeNull,
							Contents: nil,
						},
						"world": {
							Type:     ValueTypeRegular,
							Contents: []byte{},
						},
						"holà!": {
							Type:     ValueTypeRegular,
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
					actual, err := ReadNamedValues(buf, version)
					assert.EqualValues(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestWriteNamedValues(t *testing.T) {
	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    map[string]*Value
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
					map[string]*Value{},
					[]byte{0, 0},
					nil,
				},
				{
					"1 element, value empty",
					map[string]*Value{
						"": {
							Type:     ValueTypeRegular,
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
					map[string]*Value{
						"hello": {
							Type:     ValueTypeRegular,
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
				// cannot reliably test maps with more than one key because iteration order is not deterministic
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := &bytes.Buffer{}
					err := WriteNamedValues(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestLengthOfNamedValues(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]*Value
		expected int
		err      error
	}{
		{
			"nil named values",
			nil,
			LengthOfShort,
			nil,
		},
		{
			"nil named values",
			map[string]*Value{},
			LengthOfShort,
			nil,
		},
		{
			"1 element, value empty",
			map[string]*Value{
				"": {
					Type:     ValueTypeRegular,
					Contents: []byte{},
				}},
			LengthOfShort + // length of list
				LengthOfShort + // length of element key
				LengthOfInt, // length of element
			nil,
		},
		{
			"1 element, value non empty",
			map[string]*Value{
				"hello": {
					Type:     ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				}},
			LengthOfShort + // length of list
				LengthOfShort + // length of element key
				len("hello") + // contents of element key
				LengthOfInt + // length of element value
				5, // contents of element value
			nil,
		},
		{
			"3 elements",
			map[string]*Value{
				"hello": {
					Type:     ValueTypeNull,
					Contents: nil,
				},
				"world": {
					Type:     ValueTypeRegular,
					Contents: []byte{},
				},
				"holà!": {
					Type:     ValueTypeRegular,
					Contents: []byte{1, 2, 3, 4, 5},
				},
			},
			LengthOfShort + // length of list
				LengthOfShort + // length of element 1 key
				len("hello") + // contents of element 1 key
				LengthOfInt + // length of element 1 value
				LengthOfShort + // length of element 2 key
				len("world") + // contents of element 2 key
				LengthOfInt + // length of element 2 value
				LengthOfShort + // length of element 3 key
				len("holà!") + // contents of element 3 key: holà!
				LengthOfInt + // length of element 3 value
				5, // contents of element 3 value
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfNamedValues(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}
