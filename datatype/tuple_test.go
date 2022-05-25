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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTupleType(t *testing.T) {
	tupleType := NewTuple(Varchar, Int)
	assert.Equal(t, primitive.DataTypeCodeTuple, tupleType.Code())
	assert.Equal(t, []DataType{Varchar, Int}, tupleType.FieldTypes)
}

func TestTupleTypeDeepCopy(t *testing.T) {
	tt := NewTuple(Varchar, Int)
	cloned := tt.DeepCopy()
	assert.Equal(t, tt, cloned)
	cloned.FieldTypes = []DataType{Int, Uuid, Float}
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.Code())
	assert.Equal(t, []DataType{Varchar, Int}, tt.FieldTypes)
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.Code())
	assert.Equal(t, []DataType{Int, Uuid, Float}, cloned.FieldTypes)
}

func TestTupleTypeDeepCopy_NilFieldTypesSlice(t *testing.T) {
	tt := NewTuple(Varchar, Int)
	tt.FieldTypes = nil
	cloned := tt.DeepCopy()
	assert.Equal(t, tt, cloned)
	cloned.FieldTypes = []DataType{Int, Uuid, Float}
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.Code())
	assert.Nil(t, tt.FieldTypes)
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.Code())
	assert.Equal(t, []DataType{Int, Uuid, Float}, cloned.FieldTypes)
}

func TestTupleTypeDeepCopy_NilFieldType(t *testing.T) {
	tt := NewTuple(nil, Int)
	cloned := tt.DeepCopy()
	assert.Equal(t, tt, cloned)
	cloned.FieldTypes = []DataType{Int, Uuid, Float}
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.Code())
	assert.Equal(t, []DataType{nil, Int}, tt.FieldTypes)
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.Code())
	assert.Equal(t, []DataType{Int, Uuid, Float}, cloned.FieldTypes)
}

func TestTupleTypeDeepCopy_ComplexFieldTypes(t *testing.T) {
	tt := NewTuple(NewList(NewTuple(Varchar)), Int)
	cloned := tt.DeepCopy()
	assert.Equal(t, tt, cloned)
	cloned.FieldTypes[0].(*List).ElementType = NewTuple(Int)
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.Code())
	assert.Equal(t, []DataType{NewList(NewTuple(Varchar)), Int}, tt.FieldTypes)
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.Code())
	assert.Equal(t, []DataType{NewList(NewTuple(Int)), Int}, cloned.FieldTypes)
}

func TestWriteTupleType(t *testing.T) {
	tests := []struct {
		name     string
		input    DataType
		expected []byte
		err      error
	}{
		{
			"simple tuple",
			NewTuple(Varchar, Int),
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeVarchar & 0xFF),
				0, byte(primitive.DataTypeCodeInt & 0xFF),
			},
			nil,
		},
		{
			"complex tuple",
			NewTuple(NewTuple(Varchar, Int), NewTuple(Boolean, Float)),
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
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
		{"nil tuple", nil, nil, errors.New("DataType can not be nil")},
	}

	t.Run("versions_with_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var dest = &bytes.Buffer{}
						var err error
						err = WriteDataType(test.input, dest, version)
						actual := dest.Bytes()
						assert.Equal(t, test.err, err)
						assert.Equal(t, test.expected, actual)
					})
				}
			})
		}
	})

	t.Run("versions_without_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var dest = &bytes.Buffer{}
						var err error
						err = WriteDataType(test.input, dest, version)
						actual := dest.Bytes()
						require.NotNil(t, err)
						if test.err != nil {
							assert.Equal(t, test.err, err)
						} else {
							assert.Contains(t, err.Error(),
								fmt.Sprintf("invalid data type code for %s: DataTypeCode Tuple", version))
						}
						assert.Equal(t, 0, len(actual))
					})
				}
			})
		}
	})
}

func TestLengthOfTupleType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    DataType
				expected int
				err      error
			}{
				{
					"simple tuple",
					NewTuple(Varchar, Int),
					primitive.LengthOfShort * 3,
					nil,
				},
				{
					"complex tuple",
					NewTuple(NewTuple(Varchar, Int), NewTuple(Boolean, Float)),
					primitive.LengthOfShort * 9,
					nil,
				},
				{"nil tuple", nil, -1, errors.New("expected *Tuple, got <nil>")},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var actual int
					var err error
					actual, err = lengthOfTupleType(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestReadTupleType(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected DataType
		err      error
	}{
		{
			"simple tuple",
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeVarchar & 0xFF),
				0, byte(primitive.DataTypeCodeInt & 0xFF),
			},
			NewTuple(Varchar, Int),
			nil,
		},
		{
			"complex tuple",
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
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
			NewTuple(NewTuple(Varchar, Int), NewTuple(Boolean, Float)),
			nil,
		},
		{
			"cannot read tuple",
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF)},
			nil,
			fmt.Errorf("cannot read tuple field count: %w",
				fmt.Errorf("cannot read [short]: %w",
					errors.New("EOF"))),
		},
	}

	t.Run("versions_with_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {

				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var source = bytes.NewBuffer(test.input)
						var actual DataType
						var err error
						actual, err = ReadDataType(source, version)
						assert.Equal(t, test.expected, actual)
						assert.Equal(t, test.err, err)
					})
				}
			})
		}
	})

	t.Run("versions_without_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var source = bytes.NewBuffer(test.input)
						var actual DataType
						var err error
						actual, err = ReadDataType(source, version)
						require.NotNil(t, err)
						assert.Contains(t, err.Error(),
							fmt.Sprintf("invalid data type code for %s: DataTypeCode Tuple", version))
						assert.Nil(t, actual)
					})
				}
			})
		}
	})
}

func Test_tupleType_String(t1 *testing.T) {
	tests := []struct {
		name       string
		fieldTypes []DataType
		want       string
	}{
		{"empty", []DataType{}, "tuple<>"},
		{"simple", []DataType{Int, Varchar, Boolean}, "tuple<int,varchar,boolean>"},
		{"complex", []DataType{Int, NewTuple(Varchar, Boolean)}, "tuple<int,tuple<varchar,boolean>>"},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t *testing.T) {
			tuple := NewTuple(tt.fieldTypes...)
			got := tuple.AsCql()
			assert.Equal(t, tt.want, got)
		})
	}
}
