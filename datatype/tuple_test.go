package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTupleType(t *testing.T) {
	tupleType := NewTupleType(Varchar, Int)
	assert.Equal(t, primitive.DataTypeCodeTuple, tupleType.GetDataTypeCode())
	assert.Equal(t, []DataType{Varchar, Int}, tupleType.GetFieldTypes())
}

func TestTupleTypeCodecEncode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    TupleType
				expected []byte
				err      error
			}{
				{
					"simple tuple",
					NewTupleType(Varchar, Int),
					[]byte{
						0, 2, // field count
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					nil,
				},
				{
					"complex tuple",
					NewTupleType(NewTupleType(Varchar, Int), NewTupleType(Boolean, Float)),
					[]byte{
						0, 2, // field count
						0, byte(primitive.DataTypeCodeTuple & 0xFF),
						0, 2, // field count
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
						0, byte(primitive.DataTypeCodeTuple & 0xFF),
						0, 2, // field count
						0, byte(primitive.DataTypeCodeBoolean & 0xFF),
						0, byte(primitive.DataTypeCodeFloat & 0xFF),
					},
					nil,
				},
				{"nil tuple", nil, nil, errors.New("expected TupleType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeTuple)
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

func TestTupleTypeCodecEncodedLength(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    TupleType
				expected int
				err      error
			}{
				{
					"simple tuple",
					NewTupleType(Varchar, Int),
					primitive.LengthOfShort * 3,
					nil,
				},
				{
					"complex tuple",
					NewTupleType(NewTupleType(Varchar, Int), NewTupleType(Boolean, Float)),
					primitive.LengthOfShort * 9,
					nil,
				},
				{"nil tuple", nil, -1, errors.New("expected TupleType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeTuple)
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

func TestTupleTypeCodecDecode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected TupleType
				err      error
			}{
				{
					"simple tuple",
					[]byte{
						0, 2, // field count
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					NewTupleType(Varchar, Int),
					nil,
				},
				{
					"complex tuple",
					[]byte{
						0, 2, // field count
						0, byte(primitive.DataTypeCodeTuple & 0xFF),
						0, 2, // field count
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
						0, byte(primitive.DataTypeCodeTuple & 0xFF),
						0, 2, // field count
						0, byte(primitive.DataTypeCodeBoolean & 0xFF),
						0, byte(primitive.DataTypeCodeFloat & 0xFF),
					},
					NewTupleType(NewTupleType(Varchar, Int), NewTupleType(Boolean, Float)),
					nil,
				},
				{
					"cannot read tuple",
					[]byte{},
					nil,
					fmt.Errorf("cannot read tuple field count: %w",
						fmt.Errorf("cannot read [short]: %w",
							errors.New("EOF"))),
				},
			}
			codec, _ := findCodec(primitive.DataTypeCodeTuple)
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
