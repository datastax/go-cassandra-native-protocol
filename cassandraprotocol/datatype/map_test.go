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

func TestMapType(t *testing.T) {
	mapType := NewMapType(Varchar, Int)
	assert.Equal(t, cassandraprotocol.DataTypeCodeMap, mapType.GetDataTypeCode())
	assert.Equal(t, Varchar, mapType.GetKeyType())
	assert.Equal(t, Int, mapType.GetValueType())
}

func TestMapTypeCodecEncode(t *testing.T) {
	tests := []struct {
		name     string
		input    MapType
		expected []byte
		err      error
	}{
		{
			"simple map",
			NewMapType(Varchar, Int),
			[]byte{
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
			},
			nil,
		},
		{
			"complex map",
			NewMapType(NewMapType(Varchar, Int), NewMapType(Boolean, Float)),
			[]byte{
				0, byte(cassandraprotocol.DataTypeCodeMap & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeMap & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeBoolean & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeFloat & 0xFF),
			},
			nil,
		},
		{"nil map", nil, nil, errors.New("expected MapType, got <nil>")},
	}
	codec, _ := findCodec(cassandraprotocol.DataTypeCodeMap)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionMax; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					var dest = &bytes.Buffer{}
					var err error
					err = codec.encode(test.input, dest, version)
					actual := dest.Bytes()
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestMapTypeCodecEncodedLength(t *testing.T) {
	tests := []struct {
		name     string
		input    MapType
		expected int
		err      error
	}{
		{
			"simple map",
			NewMapType(Varchar, Int),
			primitives.LengthOfShort * 2,
			nil,
		},
		{
			"complex map",
			NewMapType(NewMapType(Varchar, Int), NewMapType(Boolean, Float)),
			primitives.LengthOfShort * 6,
			nil,
		},
		{"nil map", nil, -1, errors.New("expected MapType, got <nil>")},
	}
	codec, _ := findCodec(cassandraprotocol.DataTypeCodeMap)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionMax; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					var actual int
					var err error
					actual, err = codec.encodedLength(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestMapTypeCodecDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected MapType
		err      error
	}{
		{
			"simple map",
			[]byte{
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
			},
			NewMapType(Varchar, Int),
			nil,
		},
		{
			"complex map",
			[]byte{
				0, byte(cassandraprotocol.DataTypeCodeMap & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeMap & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeBoolean & 0xFF),
				0, byte(cassandraprotocol.DataTypeCodeFloat & 0xFF),
			},
			NewMapType(NewMapType(Varchar, Int), NewMapType(Boolean, Float)),
			nil,
		},
		{
			"cannot read map",
			[]byte{},
			nil,
			fmt.Errorf("cannot read map key type: %w",
				fmt.Errorf("cannot read data type code: %w",
					fmt.Errorf("cannot read [short]: %w",
						errors.New("EOF")))),
		},
	}
	codec, _ := findCodec(cassandraprotocol.DataTypeCodeMap)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionMax; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					var source = bytes.NewBuffer(test.input)
					var actual DataType
					var err error
					actual, err = codec.decode(source, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}
