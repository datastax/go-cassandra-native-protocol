package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"testing"
)

func TestCustomType(t *testing.T) {
	customType := NewCustomType("foo.bar.qix")
	assert.Equal(t, cassandraprotocol.DataTypeCodeCustom, customType.GetDataTypeCode())
	assert.Equal(t, "foo.bar.qix", customType.GetClassName())
}

func TestCustomTypeCodecEncode(t *testing.T) {
	tests := []struct {
		name     string
		input    CustomType
		expected []byte
		err      error
	}{
		{"simple custom", NewCustomType("hello"), []byte{0, 5, byte('h'), byte('e'), byte('l'), byte('l'), byte('o')}, nil},
		{"nil custom", nil, nil, errors.New("expected CustomType, got <nil>")},
	}
	codec := customTypeCodec{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionMax; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					var dest = &bytes.Buffer{}
					var err error
					err = codec.Encode(test.input, dest, version)
					actual := dest.Bytes()
					assert.Equal(t, actual, test.expected)
					assert.Equal(t, err, test.err)
				})
			}
		})
	}
}

func TestCustomTypeCodecEncodedLength(t *testing.T) {
	tests := []struct {
		name     string
		input    CustomType
		expected int
		err      error
	}{
		{"simple custom", NewCustomType("hello"), primitives.LengthOfString("hello"), nil},
		{"nil custom", nil, -1, errors.New("expected CustomType, got <nil>")},
	}
	codec := customTypeCodec{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionMax; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					var actual int
					var err error
					actual, err = codec.EncodedLength(test.input, version)
					assert.Equal(t, actual, test.expected)
					assert.Equal(t, err, test.err)
				})
			}
		})
	}
}

func TestCustomTypeCodecDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected CustomType
		err      error
	}{
		{"simple custom", []byte{0, 5, byte('h'), byte('e'), byte('l'), byte('l'), byte('o')}, NewCustomType("hello"), nil},
		{
			"cannot read custom",
			[]byte{},
			nil,
			fmt.Errorf("cannot read custom type class name: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w",
						errors.New("EOF")))),
		},
	}
	codec := customTypeCodec{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionMax; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					var source = bytes.NewBuffer(test.input)
					var actual DataType
					var err error
					actual, err = codec.Decode(source, version)
					assert.Equal(t, actual, test.expected)
					assert.Equal(t, err, test.err)
				})
			}
		})
	}
}
