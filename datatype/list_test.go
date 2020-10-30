package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListType(t *testing.T) {
	listType := NewListType(Varchar)
	assert.Equal(t, primitive.DataTypeCodeList, listType.GetDataTypeCode())
	assert.Equal(t, Varchar, listType.GetElementType())
}

func TestListTypeCodecEncode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    ListType
				expected []byte
				err      error
			}{
				{
					"simple list",
					NewListType(Varchar),
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{
					"complex list",
					NewListType(NewListType(Varchar)),
					[]byte{
						0, byte(primitive.DataTypeCodeList & 0xFF),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{"nil list", nil, nil, errors.New("expected ListType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeList)
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

func TestListTypeCodecEncodedLength(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    ListType
				expected int
				err      error
			}{
				{"simple list", NewListType(Varchar), primitive.LengthOfShort, nil},
				{"complex list", NewListType(NewListType(Varchar)), primitive.LengthOfShort + primitive.LengthOfShort, nil},
				{"nil list", nil, -1, errors.New("expected ListType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeList)
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

func TestListTypeCodecDecode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected ListType
				err      error
			}{
				{
					"simple list",
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewListType(Varchar),
					nil,
				},
				{
					"complex list",
					[]byte{
						0, byte(primitive.DataTypeCodeList & 0xff),
						0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewListType(NewListType(Varchar)),
					nil,
				},
				{
					"cannot read list",
					[]byte{},
					nil,
					fmt.Errorf("cannot read list element type: %w",
						fmt.Errorf("cannot read data type code: %w",
							fmt.Errorf("cannot read [short]: %w",
								errors.New("EOF")))),
				},
			}
			codec, _ := findCodec(primitive.DataTypeCodeList)
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
