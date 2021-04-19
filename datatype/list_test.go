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

func TestListType(t *testing.T) {
	listType := NewListType(Varchar)
	assert.Equal(t, primitive.DataTypeCodeList, listType.GetDataTypeCode())
	assert.Equal(t, Varchar, listType.GetElementType())
}

func TestListTypeClone(t *testing.T) {
	lt := NewListType(Varchar)
	clonedObj := lt.Clone().(*listType)
	assert.Equal(t, lt, clonedObj)
	clonedObj.elementType = Int
	assert.Equal(t, primitive.DataTypeCodeList, lt.GetDataTypeCode())
	assert.Equal(t, Varchar, lt.GetElementType())
	assert.Equal(t, primitive.DataTypeCodeList, clonedObj.GetDataTypeCode())
	assert.Equal(t, Int, clonedObj.GetElementType())
}

func TestWriteListType(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
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
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var dest = &bytes.Buffer{}
					var err error
					err = writeListType(test.input, dest, version)
					actual := dest.Bytes()
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestLengthOfListType(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
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
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var actual int
					var err error
					actual, err = lengthOfListType(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestReadListType(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
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
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var source = bytes.NewBuffer(test.input)
					var actual DataType
					var err error
					actual, err = readListType(source, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}
