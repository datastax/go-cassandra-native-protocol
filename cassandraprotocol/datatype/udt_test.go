package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"testing"
)

func TestUserDefinedType(t *testing.T) {
	fieldNames := []string{"f1", "f2"}
	fieldTypes := []DataType{Varchar, Int}
	udtType := NewUserDefinedType("ks1", "udt1", fieldNames, fieldTypes)
	assert.Equal(t, cassandraprotocol.DataTypeCodeUdt, udtType.GetDataTypeCode())
	assert.Equal(t, fieldTypes, udtType.GetFieldTypes())
}

func TestUserDefinedTypeCodecEncode(t *testing.T) {
	tests := []struct {
		name     string
		input    UserDefinedType
		expected []byte
		err      error
	}{
		{
			"simple udt",
			NewUserDefinedType("ks1", "udt1", []string{"f1", "f2"}, []DataType{Varchar, Int}),
			[]byte{
				0, 3, byte('k'), byte('s'), byte('1'),
				0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
				0, 2, // field count
				0, 2, byte('f'), byte('1'),
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, 2, byte('f'), byte('2'),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
			},
			nil,
		},
		{
			"complex udt",
			NewUserDefinedType("ks1", "udt1", []string{"f1"}, []DataType{
				NewUserDefinedType("ks1", "udt2", []string{"f2", "f3"}, []DataType{Varchar, Int}),
			}),
			[]byte{
				0, 3, byte('k'), byte('s'), byte('1'),
				0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
				0, 1, // field count
				0, 2, byte('f'), byte('1'),
				0, byte(cassandraprotocol.DataTypeCodeUdt & 0xFF),
				0, 3, byte('k'), byte('s'), byte('1'),
				0, 4, byte('u'), byte('d'), byte('t'), byte('2'),
				0, 2, // field count
				0, 2, byte('f'), byte('2'),
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, 2, byte('f'), byte('3'),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
			},
			nil,
		},
		{"nil udt", nil, nil, errors.New("expected UserDefinedType, got <nil>")},
	}
	codec, _ := findCodec(cassandraprotocol.DataTypeCodeUdt)
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

func TestUserDefinedTypeCodecEncodedLength(t *testing.T) {
	tests := []struct {
		name     string
		input    UserDefinedType
		expected int
		err      error
	}{
		{
			"simple udt",
			NewUserDefinedType("ks1", "udt1", []string{"f1", "f2"}, []DataType{Varchar, Int}),
			primitives.LengthOfString("ks1") +
				primitives.LengthOfString("udt1") +
				primitives.LengthOfShort + // field count
				primitives.LengthOfString("f1") +
				primitives.LengthOfShort + // varchar
				primitives.LengthOfString("f2") +
				primitives.LengthOfShort, // int
			nil,
		},
		{
			"complex udt",
			NewUserDefinedType("ks1", "udt1", []string{"f1"}, []DataType{
				NewUserDefinedType("ks1", "udt2", []string{"f2", "f3"}, []DataType{Varchar, Int}),
			}),
			primitives.LengthOfString("ks1") +
				primitives.LengthOfString("udt1") +
				primitives.LengthOfShort + // field count
				primitives.LengthOfString("f1") +
				primitives.LengthOfShort + // UDT
				primitives.LengthOfString("ks1") +
				primitives.LengthOfString("udt2") +
				primitives.LengthOfShort + // field count
				primitives.LengthOfString("f2") +
				primitives.LengthOfShort + // varchar
				primitives.LengthOfString("f3") +
				primitives.LengthOfShort, // int
			nil,
		},
		{"nil udt", nil, -1, errors.New("expected UserDefinedType, got <nil>")},
	}
	codec, _ := findCodec(cassandraprotocol.DataTypeCodeUdt)
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

func TestUserDefinedTypeCodecDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected UserDefinedType
		err      error
	}{
		{
			"simple udt",
			[]byte{
				0, 3, byte('k'), byte('s'), byte('1'),
				0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
				0, 2, // field count
				0, 2, byte('f'), byte('1'),
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, 2, byte('f'), byte('2'),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
			},
			NewUserDefinedType("ks1", "udt1", []string{"f1", "f2"}, []DataType{Varchar, Int}),
			nil,
		},
		{
			"complex udt",
			[]byte{
				0, 3, byte('k'), byte('s'), byte('1'),
				0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
				0, 1, // field count
				0, 2, byte('f'), byte('1'),
				0, byte(cassandraprotocol.DataTypeCodeUdt & 0xFF),
				0, 3, byte('k'), byte('s'), byte('1'),
				0, 4, byte('u'), byte('d'), byte('t'), byte('2'),
				0, 2, // field count
				0, 2, byte('f'), byte('2'),
				0, byte(cassandraprotocol.DataTypeCodeVarchar & 0xFF),
				0, 2, byte('f'), byte('3'),
				0, byte(cassandraprotocol.DataTypeCodeInt & 0xFF),
			},
			NewUserDefinedType("ks1", "udt1", []string{"f1"}, []DataType{
				NewUserDefinedType("ks1", "udt2", []string{"f2", "f3"}, []DataType{Varchar, Int}),
			}),
			nil,
		},
		{
			"cannot read udt",
			[]byte{},
			nil,
			fmt.Errorf("cannot read udt keyspace: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w",
						errors.New("EOF")))),
		},
	}
	codec, _ := findCodec(cassandraprotocol.DataTypeCodeUdt)
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
