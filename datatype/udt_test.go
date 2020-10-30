package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUserDefinedType(t *testing.T) {
	fieldNames := []string{"f1", "f2"}
	fieldTypes := []DataType{Varchar, Int}
	udtType, err := NewUserDefinedType("ks1", "udt1", fieldNames, fieldTypes)
	assert.Nil(t, err)
	assert.Equal(t, primitive.DataTypeCodeUdt, udtType.GetDataTypeCode())
	assert.Equal(t, fieldTypes, udtType.GetFieldTypes())
	udtType2, err2 := NewUserDefinedType("ks1", "udt1", fieldNames, []DataType{Varchar, Int, Boolean})
	assert.Nil(t, udtType2)
	assert.Errorf(t, err2, "field names and field types length mismatch: 2 != 3")
}

var udt1, _ = NewUserDefinedType("ks1", "udt1", []string{"f1", "f2"}, []DataType{Varchar, Int})
var udt2, _ = NewUserDefinedType("ks1", "udt2", []string{"f1"}, []DataType{udt1})

func TestUserDefinedTypeCodecEncode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    UserDefinedType
				expected []byte
				err      error
			}{
				{
					"simple udt",
					udt1,
					[]byte{
						0, 3, byte('k'), byte('s'), byte('1'),
						0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
						0, 2, // field count
						0, 2, byte('f'), byte('1'),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, 2, byte('f'), byte('2'),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					nil,
				},
				{
					"complex udt",
					udt2,
					[]byte{
						0, 3, byte('k'), byte('s'), byte('1'),
						0, 4, byte('u'), byte('d'), byte('t'), byte('2'),
						0, 1, // field count
						0, 2, byte('f'), byte('1'),
						0, byte(primitive.DataTypeCodeUdt & 0xFF),
						0, 3, byte('k'), byte('s'), byte('1'),
						0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
						0, 2, // field count
						0, 2, byte('f'), byte('1'),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, 2, byte('f'), byte('2'),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					nil,
				},
				{"nil udt", nil, nil, errors.New("expected UserDefinedType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeUdt)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
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
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    UserDefinedType
				expected int
				err      error
			}{
				{
					"simple udt",
					udt1,
					primitive.LengthOfString("ks1") +
						primitive.LengthOfString("udt1") +
						primitive.LengthOfShort + // field count
						primitive.LengthOfString("f1") +
						primitive.LengthOfShort + // varchar
						primitive.LengthOfString("f2") +
						primitive.LengthOfShort, // int
					nil,
				},
				{
					"complex udt",
					udt2,
					primitive.LengthOfString("ks1") +
						primitive.LengthOfString("udt2") +
						primitive.LengthOfShort + // field count
						primitive.LengthOfString("f1") +
						primitive.LengthOfShort + // UDT
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("udt1") +
						primitive.LengthOfShort + // field count
						primitive.LengthOfString("f1") +
						primitive.LengthOfShort + // varchar
						primitive.LengthOfString("f2") +
						primitive.LengthOfShort, // int
					nil,
				},
				{"nil udt", nil, -1, errors.New("expected UserDefinedType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeUdt)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
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
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
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
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, 2, byte('f'), byte('2'),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					udt1,
					nil,
				},
				{
					"complex udt",
					[]byte{
						0, 3, byte('k'), byte('s'), byte('1'),
						0, 4, byte('u'), byte('d'), byte('t'), byte('2'),
						0, 1, // field count
						0, 2, byte('f'), byte('1'),
						0, byte(primitive.DataTypeCodeUdt & 0xFF),
						0, 3, byte('k'), byte('s'), byte('1'),
						0, 4, byte('u'), byte('d'), byte('t'), byte('1'),
						0, 2, // field count
						0, 2, byte('f'), byte('1'),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, 2, byte('f'), byte('2'),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					udt2,
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
			codec, _ := findCodec(primitive.DataTypeCodeUdt)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
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
