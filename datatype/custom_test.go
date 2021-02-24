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

func TestCustomType(t *testing.T) {
	customType := NewCustomType("foo.bar.qix")
	assert.Equal(t, primitive.DataTypeCodeCustom, customType.GetDataTypeCode())
	assert.Equal(t, "foo.bar.qix", customType.GetClassName())
}

func TestCustomTypeCodecEncode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    CustomType
				expected []byte
				err      error
			}{
				{"simple custom", NewCustomType("hello"), []byte{0, 5, byte('h'), byte('e'), byte('l'), byte('l'), byte('o')}, nil},
				{"nil custom", nil, nil, errors.New("expected CustomType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeCustom)
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

func TestCustomTypeCodecEncodedLength(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    CustomType
				expected int
				err      error
			}{
				{"simple custom", NewCustomType("hello"), primitive.LengthOfString("hello"), nil},
				{"nil custom", nil, -1, errors.New("expected CustomType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeCustom)
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

func TestCustomTypeCodecDecode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected CustomType
				err      error
			}{
				{"simple custom", []byte{0, 5, byte('h'), byte('e'), byte('l'), byte('l'), byte('o')}, NewCustomType("hello"), nil},
				{
					"cannot read custom",
					[]byte{},
					nil,
					fmt.Errorf("cannot read custom type class name: %w",
						fmt.Errorf("cannot read [string] length: %w",
							fmt.Errorf("cannot read [short]: %w",
								errors.New("EOF")))),
				},
			}
			codec, _ := findCodec(primitive.DataTypeCodeCustom)
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

func TestCustomTypeClone(t *testing.T) {
	ct := NewCustomType("foo.bar.qix")
	clonedCustomType := ct.Clone().(*customType)
	clonedCustomType.className = "123"
	assert.Equal(t, "123", clonedCustomType.GetClassName())
	assert.Equal(t, "foo.bar.qix", ct.GetClassName())
}
