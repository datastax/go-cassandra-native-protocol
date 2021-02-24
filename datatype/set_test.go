// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetType(t *testing.T) {
	setType := NewSetType(Varchar)
	assert.Equal(t, primitive.DataTypeCodeSet, setType.GetDataTypeCode())
	assert.Equal(t, Varchar, setType.GetElementType())
}

func TestSetTypeClone(t *testing.T) {
	st := NewSetType(Varchar)
	cloned := st.Clone().(*setType)
	cloned.elementType = Int
	assert.Equal(t, primitive.DataTypeCodeSet, st.GetDataTypeCode())
	assert.Equal(t, Varchar, st.GetElementType())
	assert.Equal(t, primitive.DataTypeCodeSet, cloned.GetDataTypeCode())
	assert.Equal(t, Int, cloned.GetElementType())
}

func TestSetTypeCodecEncode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    SetType
				expected []byte
				err      error
			}{
				{
					"simple set",
					NewSetType(Varchar),
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{
					"complex set",
					NewSetType(NewSetType(Varchar)),
					[]byte{
						0, byte(primitive.DataTypeCodeSet & 0xFF),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{"nil set", nil, nil, errors.New("expected SetType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeSet)
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

func TestSetTypeCodecEncodedLength(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    SetType
				expected int
				err      error
			}{
				{"simple set", NewSetType(Varchar), primitive.LengthOfShort, nil},
				{"complex set", NewSetType(NewSetType(Varchar)), primitive.LengthOfShort + primitive.LengthOfShort, nil},
				{"nil set", nil, -1, errors.New("expected SetType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeSet)
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

func TestSetTypeCodecDecode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected SetType
				err      error
			}{
				{
					"simple set",
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewSetType(Varchar),
					nil,
				},
				{
					"complex set",
					[]byte{
						0, byte(primitive.DataTypeCodeSet & 0xff),
						0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewSetType(NewSetType(Varchar)),
					nil,
				},
				{
					"cannot read set",
					[]byte{},
					nil,
					fmt.Errorf("cannot read set element type: %w",
						fmt.Errorf("cannot read data type code: %w",
							fmt.Errorf("cannot read [short]: %w",
								errors.New("EOF")))),
				},
			}
			codec, _ := findCodec(primitive.DataTypeCodeSet)
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
