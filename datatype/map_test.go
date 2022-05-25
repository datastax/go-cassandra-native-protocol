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

func TestMapType(t *testing.T) {
	mapType := NewMap(Varchar, Int)
	assert.Equal(t, primitive.DataTypeCodeMap, mapType.GetDataTypeCode())
	assert.Equal(t, Varchar, mapType.KeyType)
	assert.Equal(t, Int, mapType.ValueType)
}

func TestMapTypeDeepCopy(t *testing.T) {
	mt := NewMap(Varchar, Int)
	cloned := mt.DeepCopy()
	assert.Equal(t, mt, cloned)
	cloned.KeyType = Inet
	cloned.ValueType = Uuid
	assert.Equal(t, primitive.DataTypeCodeMap, mt.GetDataTypeCode())
	assert.Equal(t, Varchar, mt.KeyType)
	assert.Equal(t, Int, mt.ValueType)
	assert.Equal(t, primitive.DataTypeCodeMap, cloned.GetDataTypeCode())
	assert.Equal(t, Inet, cloned.KeyType)
	assert.Equal(t, Uuid, cloned.ValueType)
}

func TestWriteMapType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    DataType
				expected []byte
				err      error
			}{
				{
					"simple map",
					NewMap(Varchar, Int),
					[]byte{
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					nil,
				},
				{
					"complex map",
					NewMap(NewMap(Varchar, Int), NewMap(Boolean, Float)),
					[]byte{
						0, byte(primitive.DataTypeCodeMap & 0xFF),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
						0, byte(primitive.DataTypeCodeMap & 0xFF),
						0, byte(primitive.DataTypeCodeBoolean & 0xFF),
						0, byte(primitive.DataTypeCodeFloat & 0xFF),
					},
					nil,
				},
				{"nil map", nil, nil, errors.New("expected *Map, got <nil>")},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var dest = &bytes.Buffer{}
					var err error
					err = writeMapType(test.input, dest, version)
					actual := dest.Bytes()
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestLengthOfMapType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    DataType
				expected int
				err      error
			}{
				{
					"simple map",
					NewMap(Varchar, Int),
					primitive.LengthOfShort * 2,
					nil,
				},
				{
					"complex map",
					NewMap(NewMap(Varchar, Int), NewMap(Boolean, Float)),
					primitive.LengthOfShort * 6,
					nil,
				},
				{"nil map", nil, -1, errors.New("expected *Map, got <nil>")},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var actual int
					var err error
					actual, err = lengthOfMapType(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestReadMapType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected DataType
				err      error
			}{
				{
					"simple map",
					[]byte{
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
					},
					NewMap(Varchar, Int),
					nil,
				},
				{
					"complex map",
					[]byte{
						0, byte(primitive.DataTypeCodeMap & 0xFF),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF),
						0, byte(primitive.DataTypeCodeInt & 0xFF),
						0, byte(primitive.DataTypeCodeMap & 0xFF),
						0, byte(primitive.DataTypeCodeBoolean & 0xFF),
						0, byte(primitive.DataTypeCodeFloat & 0xFF),
					},
					NewMap(NewMap(Varchar, Int), NewMap(Boolean, Float)),
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
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var source = bytes.NewBuffer(test.input)
					var actual DataType
					var err error
					actual, err = readMapType(source, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}
