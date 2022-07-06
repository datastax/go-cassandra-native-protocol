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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestSetType(t *testing.T) {
	setType := NewSet(Varchar)
	assert.Equal(t, primitive.DataTypeCodeSet, setType.Code())
	assert.Equal(t, Varchar, setType.ElementType)
}

func TestSetTypeDeepCopy(t *testing.T) {
	st := NewSet(Varchar)
	cloned := st.DeepCopy()
	assert.Equal(t, st, cloned)
	cloned.ElementType = Int
	assert.Equal(t, primitive.DataTypeCodeSet, st.Code())
	assert.Equal(t, Varchar, st.ElementType)
	assert.Equal(t, primitive.DataTypeCodeSet, cloned.Code())
	assert.Equal(t, Int, cloned.ElementType)
}

func TestWriteSetType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    DataType
				expected []byte
				err      error
			}{
				{
					"simple set",
					NewSet(Varchar),
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{
					"complex set",
					NewSet(NewSet(Varchar)),
					[]byte{
						0, byte(primitive.DataTypeCodeSet & 0xFF),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{"nil set", nil, nil, errors.New("expected *Set, got <nil>")},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var dest = &bytes.Buffer{}
					var err error
					err = writeSetType(test.input, dest, version)
					actual := dest.Bytes()
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestLengthOfSetType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    DataType
				expected int
				err      error
			}{
				{"simple set", NewSet(Varchar), primitive.LengthOfShort, nil},
				{"complex set", NewSet(NewSet(Varchar)), primitive.LengthOfShort + primitive.LengthOfShort, nil},
				{"nil set", nil, -1, errors.New("expected *Set, got <nil>")},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var actual int
					var err error
					actual, err = lengthOfSetType(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestReadSetType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected DataType
				err      error
			}{
				{
					"simple set",
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewSet(Varchar),
					nil,
				},
				{
					"complex set",
					[]byte{
						0, byte(primitive.DataTypeCodeSet & 0xff),
						0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewSet(NewSet(Varchar)),
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
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var source = bytes.NewBuffer(test.input)
					var actual DataType
					var err error
					actual, err = readSetType(source, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}
