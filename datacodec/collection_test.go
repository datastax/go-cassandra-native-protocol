// Copyright 2021 DataStax
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

package datacodec

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math"
	"strconv"
	"testing"
)

var (
	listOfInt, _          = NewList(datatype.NewListType(datatype.Int))
	listOfSetOfVarchar, _ = NewList(datatype.NewListType(datatype.NewSetType(datatype.Varchar)))
)

var (
	listOneBytes4 = []byte{
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 1,
	}
	listOneBytes2 = []byte{
		0, 1,
		0, 0, 0, 4,
		0, 0, 0, 1,
	}
	listOneTwoThreeBytes2 = []byte{
		0, 3,
		0, 0, 0, 4,
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 2,
		0, 0, 0, 4,
		0, 0, 0, 3,
	}
	listOneTwoThreeBytes4 = []byte{
		0, 0, 0, 3,
		0, 0, 0, 4,
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 2,
		0, 0, 0, 4,
		0, 0, 0, 3,
	}
	listAbcDefBytes2 = []byte{
		0, 2, // length of outer collection
		0, 0, 0, 9, // length of outer collection 1st element
		0, 1, // length of 1st inner collection
		0, 0, 0, 3, // length of 1st inner collection 1st element
		a, b, c, // element
		0, 0, 0, 9, // length of outer collection 2nd element
		0, 1, // length of 2nd inner collection
		0, 0, 0, 3, // length of 2nd inner collection 1st element
		d, e, f, // element
	}
	listAbcDefBytes4 = []byte{
		0, 0, 0, 2, // length of outer collection
		0, 0, 0, 11, // length of outer collection 1st element
		0, 0, 0, 1, // length of 1st inner collection
		0, 0, 0, 3, // length of 1st inner collection 1st element
		a, b, c, // element
		0, 0, 0, 11, // length of outer collection 2nd element
		0, 0, 0, 1, // length of 2nd inner collection
		0, 0, 0, 3, // length of 2nd inner collection 1st element
		d, e, f, // element
	}
	listAbcDefEmptyBytes2 = []byte{
		0, 2, // length of outer collection
		0, 0, 0, 16, // length of outer collection 1st element
		0, 2, // length of 1st inner collection
		0, 0, 0, 3, // length of 1st inner collection 1st element
		a, b, c, // element
		0, 0, 0, 3, // length of 1st inner collection 2nd element
		d, e, f, // element
		0, 0, 0, 2, // length of outer collection 2nd element
		0, 0, // length of 2nd inner collection
	}
	listAbcDefEmptyBytes4 = []byte{
		0, 0, 0, 2, // length of outer collection
		0, 0, 0, 18, // length of outer collection 1st element
		0, 0, 0, 2, // length of 1st inner collection
		0, 0, 0, 3, // length of 1st inner collection 1st element
		a, b, c, // element
		0, 0, 0, 3, // length of 1st inner collection 2nd element
		d, e, f, // element
		0, 0, 0, 4, // length of outer collection 2nd element
		0, 0, 0, 0, // length of 2nd inner collection
	}
	// lists with null elements can happen when retrieving the TTLs of some collection
	listOneTwoNullBytes4 = []byte{
		0, 0, 0, 3,
		0, 0, 0, 4,
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 2,
		255, 255, 255, 255,
	}
	listOneTwoNullBytes2 = []byte{
		0, 3,
		0, 0, 0, 4,
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 2,
		255, 255, 255, 255,
	}
	listAbcNullNullBytes4 = []byte{
		0, 0, 0, 2, // length of outer collection
		0, 0, 0, 15, // length of outer collection 1st element
		0, 0, 0, 2, // length of 1st inner collection
		0, 0, 0, 3, // length of 1st inner collection 1st element
		a, b, c, // element
		255, 255, 255, 255, // inner collection 2nd element (null)
		255, 255, 255, 255, // outer collection 2nd element (null)
	}
	listAbcNullNullBytes2 = []byte{
		0, 2, // length of outer collection
		0, 0, 0, 13, // length of outer collection 1st element
		0, 2, // length of 1st inner collection
		0, 0, 0, 3, // length of 1st inner collection 1st element
		a, b, c, // element
		255, 255, 255, 255, // inner collection 2nd element (null)
		255, 255, 255, 255, // outer collection 2nd element (null)
	}
)

func TestNewList(t *testing.T) {
	tests := []struct {
		name     string
		dataType datatype.ListType
		want     Codec
		wantErr  string
	}{
		{
			"nil",
			nil,
			nil,
			"data type is nil",
		},
		{
			"simple",
			datatype.NewListType(datatype.Int),
			&collectionCodec{
				dataType:     datatype.NewListType(datatype.Int),
				elementCodec: &intCodec{},
			},
			"",
		},
		{
			"complex",
			datatype.NewListType(datatype.NewListType(datatype.Int)),
			&collectionCodec{
				dataType: datatype.NewListType(datatype.NewListType(datatype.Int)),
				elementCodec: &collectionCodec{
					dataType:     datatype.NewListType(datatype.Int),
					elementCodec: &intCodec{},
				},
			},
			"",
		},
		{
			"wrong data type",
			datatype.NewListType(wrongDataType{}),
			nil,
			"cannot create codec for list elements: cannot create data codec for CQL type 666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := NewList(tt.dataType)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func TestNewSet(t *testing.T) {
	tests := []struct {
		name     string
		dataType datatype.SetType
		want     Codec
		wantErr  string
	}{
		{
			"nil",
			nil,
			nil,
			"data type is nil",
		},
		{
			"simple",
			datatype.NewSetType(datatype.Int),
			&collectionCodec{
				dataType:     datatype.NewSetType(datatype.Int),
				elementCodec: &intCodec{},
			},
			"",
		},
		{
			"complex",
			datatype.NewSetType(datatype.NewSetType(datatype.Int)),
			&collectionCodec{
				dataType: datatype.NewSetType(datatype.NewSetType(datatype.Int)),
				elementCodec: &collectionCodec{
					dataType:     datatype.NewSetType(datatype.Int),
					elementCodec: &intCodec{},
				},
			},
			"",
		},
		{
			"wrong data type",
			datatype.NewSetType(wrongDataType{}),
			nil,
			"cannot create codec for set elements: cannot create data codec for CQL type 666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := NewSet(tt.dataType)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_collectionCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   interface{}
				expected []byte
				err      string
			}{
				{"list<int> nil untyped", listOfInt, nil, nil, ""},
				{"list<int> nil slice", listOfInt, new([]int), nil, ""},
				{"list<int> empty", listOfInt, []int{}, []byte{0, 0, 0, 0}, ""},
				{"list<int> one elem", listOfInt, []int{1}, listOneBytes4, ""},
				{"list<int> one elem array", listOfInt, [1]int{1}, listOneBytes4, ""},
				{"list<int> many elems", listOfInt, []int{1, 2, 3}, listOneTwoThreeBytes4, ""},
				{"list<int> pointer slice", listOfInt, &[]int{1, 2, 3}, listOneTwoThreeBytes4, ""},
				{"list<int> many elems pointers", listOfInt, []*int{intPtr(1), intPtr(2), intPtr(3)}, listOneTwoThreeBytes4, ""},
				{"list<int> pointer slice pointer elems", listOfInt, &[]*int{intPtr(1), intPtr(2), intPtr(3)}, listOneTwoThreeBytes4, ""},
				{"list<int> many elems []interface{}", listOfInt, []interface{}{1, 2, 3}, listOneTwoThreeBytes4, ""},
				{"list<int> many elems interface{}", listOfInt, interface{}([]interface{}{1, 2, 3}), listOneTwoThreeBytes4, ""},
				{"list<int> nil element", listOfInt, []interface{}{nil}, []byte{0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0xff, 0xff}, ""},
				{"list<int> wrong source type", listOfInt, 123, nil, fmt.Sprintf("cannot encode int as CQL %s with %s: source type not supported", listOfInt.DataType(), version)},
				{"list<int> wrong source type nil", listOfInt, map[string]int(nil), nil, fmt.Sprintf("cannot encode map[string]int as CQL %s with %s: source type not supported", listOfInt.DataType(), version)},
				{"list<int> wrong source type nil pointer", listOfInt, new(map[string]int), nil, fmt.Sprintf("cannot encode *map[string]int as CQL %s with %s: source type not supported", listOfInt.DataType(), version)},
				{"list<set<text>> nil untyped", listOfSetOfVarchar, nil, nil, ""},
				{"list<set<text>> nil slice", listOfSetOfVarchar, [][]string(nil), nil, ""},
				{"list<set<text>> empty", listOfSetOfVarchar, [][]string{}, []byte{0, 0, 0, 0}, ""},
				{"list<set<text>> many elems", listOfSetOfVarchar, [][]string{{"abc"}, {"def"}}, listAbcDefBytes4, ""},
				{"list<set<text>> pointer array", listOfSetOfVarchar, &[2][1]string{{"abc"}, {"def"}}, listAbcDefBytes4, ""},
				{"list<set<text>> pointers", listOfSetOfVarchar, &[][]*string{{stringPtr("abc"), stringPtr("def")}, {}}, listAbcDefEmptyBytes4, ""},
				{"list<set<text>> many elems interface{}", listOfSetOfVarchar, [][]interface{}{{"abc", "def"}, {}}, listAbcDefEmptyBytes4, ""},
				{"list<set<text>> nil element", listOfSetOfVarchar, []interface{}{nil}, []byte{0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0xff, 0xff}, ""},
				{"list<set<text>> nil inner element", listOfSetOfVarchar, []interface{}{[]interface{}{nil}}, []byte{0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0xff, 0xff}, ""},
				{"list<set<text>> wrong source type", listOfSetOfVarchar, 123, nil, fmt.Sprintf("cannot encode int as CQL %s with %s: source type not supported", listOfSetOfVarchar.DataType(), version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := tt.codec.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   interface{}
				expected []byte
				err      string
			}{
				{"list<int> nil untyped", listOfInt, nil, nil, ""},
				{"list<int> nil slice", listOfInt, []int(nil), nil, ""},
				{"list<int> empty", listOfInt, []int{}, []byte{0, 0}, ""},
				{"list<int> one elem", listOfInt, []int{1}, listOneBytes2, ""},
				{"list<int> one elem array", listOfInt, [1]int{1}, listOneBytes2, ""},
				{"list<int> many elems", listOfInt, []int{1, 2, 3}, listOneTwoThreeBytes2, ""},
				{"list<int> many elems pointers", listOfInt, []*int{intPtr(1), intPtr(2), intPtr(3)}, listOneTwoThreeBytes2, ""},
				{"list<int> many elems interface{}", listOfInt, []interface{}{1, 2, 3}, listOneTwoThreeBytes2, ""},
				{"list<int> nil element", listOfInt, []interface{}{nil}, []byte{0x0, 0x1, 0xff, 0xff, 0xff, 0xff}, ""},
				{"list<int> wrong source type", listOfInt, 123, nil, fmt.Sprintf("cannot encode int as CQL %s with %s: source type not supported", listOfInt.DataType(), version)},
				{"list<int> wrong source type nil", listOfInt, map[string]int(nil), nil, fmt.Sprintf("cannot encode map[string]int as CQL %s with %s: source type not supported", listOfInt.DataType(), version)},
				{"list<set<text>> nil untyped", listOfSetOfVarchar, nil, nil, ""},
				{"list<set<text>> nil slice", listOfSetOfVarchar, [][]string(nil), nil, ""},
				{"list<set<text>> empty", listOfSetOfVarchar, [][]string{}, []byte{0, 0}, ""},
				{"list<set<text>> many elems", listOfSetOfVarchar, [][]string{{"abc"}, {"def"}}, listAbcDefBytes2, ""},
				{"list<set<text>> array", listOfSetOfVarchar, [2][1]string{{"abc"}, {"def"}}, listAbcDefBytes2, ""},
				{"list<set<text>> pointers", listOfSetOfVarchar, [][]*string{{stringPtr("abc"), stringPtr("def")}, {}}, listAbcDefEmptyBytes2, ""},
				{"list<set<text>> many elems interface{}", listOfSetOfVarchar, [][]interface{}{{"abc", "def"}, {}}, listAbcDefEmptyBytes2, ""},
				{"list<set<text>> nil element", listOfSetOfVarchar, []interface{}{nil}, []byte{0x0, 0x1, 0xff, 0xff, 0xff, 0xff}, ""},
				{"list<set<text>> nil inner element", listOfSetOfVarchar, []interface{}{[]interface{}{nil}}, []byte{0x0, 0x1, 0x0, 0x0, 0x0, 0x6, 0x0, 0x1, 0xff, 0xff, 0xff, 0xff}, ""},
				{"list<set<text>> wrong source type", listOfSetOfVarchar, 123, nil, fmt.Sprintf("cannot encode int as CQL %s with %s: source type not supported", listOfSetOfVarchar.DataType(), version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := tt.codec.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_collectionCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   []byte
				dest     interface{}
				want     interface{}
				wantNull bool
				err      string
			}{
				{"list<int> nil untyped", listOfInt, nil, nil, nil, true, fmt.Sprintf("cannot decode CQL list<int> as <nil> with %v: destination is nil", version)},
				{"list<int> nil slice", listOfInt, nil, new([]int), new([]int), true, ""},
				{"list<int> empty", listOfInt, []byte{0, 0, 0, 0}, new([]int), &[]int{}, false, ""},
				{"list<int> one elem", listOfInt, listOneBytes4, new([]int), &[]int{1}, false, ""},
				{"list<int> one elem array", listOfInt, listOneBytes4, new([1]int), &[1]int{1}, false, ""},
				{"list<int> many elems", listOfInt, listOneTwoThreeBytes4, new([]int), &[]int{1, 2, 3}, false, ""},
				{"list<int> many elems pointers", listOfInt, listOneTwoThreeBytes4, new([]*int), &[]*int{intPtr(1), intPtr(2), intPtr(3)}, false, ""},
				{"list<int> many elems []interface{}", listOfInt, listOneTwoThreeBytes4, new([]interface{}), &[]interface{}{int32(1), int32(2), int32(3)}, false, ""},
				{"list<int> many elems interface{}", listOfInt, listOneTwoThreeBytes4, new(interface{}), interfacePtr([]*int32{int32Ptr(1), int32Ptr(2), int32Ptr(3)}), false, ""},
				{"list<int> nil elem", listOfInt, listOneTwoNullBytes4, new([]int), &[]int{1, 2, 0}, false, ""},
				{"list<int> nil elem pointers", listOfInt, listOneTwoNullBytes4, new([]*int), &[]*int{intPtr(1), intPtr(2), nil}, false, ""},
				{"list<int> nil elem []interface{}", listOfInt, listOneTwoNullBytes4, new([]interface{}), &[]interface{}{int32(1), int32(2), nil}, false, ""},
				{"list<int> nil elem interface{}", listOfInt, listOneTwoNullBytes4, new(interface{}), interfacePtr([]*int32{int32Ptr(1), int32Ptr(2), nil}), false, ""},
				{"list<int> pointer required", listOfInt, nil, []interface{}{}, []interface{}{}, true, fmt.Sprintf("cannot decode CQL %s as []interface {} with %s: destination is not pointer", listOfInt.DataType(), version)},
				{"list<int> wrong destination type", listOfInt, nil, &map[string]int{}, new(map[string]int), true, fmt.Sprintf("cannot decode CQL %s as *map[string]int with %s: destination type not supported", listOfInt.DataType(), version)},
				{"list<set<text>> nil untyped", listOfSetOfVarchar, nil, nil, nil, true, fmt.Sprintf("cannot decode CQL list<set<varchar>> as <nil> with %v: destination is nil", version)},
				{"list<set<text>> nil slice", listOfSetOfVarchar, nil, new([][]string), new([][]string), true, ""},
				{"list<set<text>> empty", listOfSetOfVarchar, []byte{0, 0, 0, 0}, new([][]string), &[][]string{}, false, ""},
				{"list<set<text>> many elems", listOfSetOfVarchar, listAbcDefBytes4, new([][]string), &[][]string{{"abc"}, {"def"}}, false, ""},
				{"list<set<text>> array", listOfSetOfVarchar, listAbcDefBytes4, new([2][1]string), &[2][1]string{{"abc"}, {"def"}}, false, ""},
				{"list<set<text>> pointers", listOfSetOfVarchar, listAbcDefEmptyBytes4, new([][]*string), &[][]*string{{stringPtr("abc"), stringPtr("def")}, {}}, false, ""},
				{"list<set<text>> many elems [][]interface{}", listOfSetOfVarchar, listAbcDefEmptyBytes4, new([][]interface{}), &[][]interface{}{{"abc", "def"}, {}}, false, ""},
				{"list<set<text>> many elems interface{}", listOfSetOfVarchar, listAbcDefEmptyBytes4, new(interface{}), interfacePtr([][]*string{{stringPtr("abc"), stringPtr("def")}, {}}), false, ""},
				{"list<set<text>> nil elem", listOfSetOfVarchar, listAbcNullNullBytes4, new([][]string), &[][]string{{"abc", ""}, nil}, false, ""},
				{"list<set<text>> nil elem pointers", listOfSetOfVarchar, listAbcNullNullBytes4, new([][]*string), &[][]*string{{stringPtr("abc"), nil}, nil}, false, ""},
				{"list<set<text>> nil elem [][]interface{}", listOfSetOfVarchar, listAbcNullNullBytes4, new([][]interface{}), &[][]interface{}{{"abc", nil}, nil}, false, ""},
				{"list<set<text>> nil elem interface{}", listOfSetOfVarchar, listAbcNullNullBytes4, new(interface{}), interfacePtr([][]*string{{stringPtr("abc"), nil}, nil}), false, ""},
				{"pointer required", listOfSetOfVarchar, nil, [][]interface{}{}, [][]interface{}{}, true, fmt.Sprintf("cannot decode CQL %s as [][]interface {} with %s: destination is not pointer", listOfSetOfVarchar.DataType(), version)},
				{"list<set<text>> wrong destination type", listOfSetOfVarchar, nil, &map[string]int{}, new(map[string]int), true, fmt.Sprintf("cannot decode CQL %s as *map[string]int with %s: destination type not supported", listOfSetOfVarchar.DataType(), version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := tt.codec.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.want, tt.dest)
					assert.Equal(t, tt.wantNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   []byte
				dest     interface{}
				want     interface{}
				wantNull bool
				err      string
			}{
				{"list<int> nil untyped", listOfInt, nil, nil, nil, true, "cannot decode CQL list<int> as <nil> with ProtocolVersion OSS 2: destination is nil"},
				{"list<int> nil slice", listOfInt, nil, new([]int), new([]int), true, ""},
				{"list<int> empty", listOfInt, []byte{0, 0}, new([]int), &[]int{}, false, ""},
				{"list<int> one elem", listOfInt, listOneBytes2, new([]int), &[]int{1}, false, ""},
				{"list<int> one elem array", listOfInt, listOneBytes2, new([1]int), &[1]int{1}, false, ""},
				{"list<int> many elems", listOfInt, listOneTwoThreeBytes2, new([]int), &[]int{1, 2, 3}, false, ""},
				{"list<int> many elems pointers", listOfInt, listOneTwoThreeBytes2, new([]*int), &[]*int{intPtr(1), intPtr(2), intPtr(3)}, false, ""},
				{"list<int> many elems []interface{}", listOfInt, listOneTwoThreeBytes2, new([]interface{}), &[]interface{}{int32(1), int32(2), int32(3)}, false, ""},
				{"list<int> many elems interface{}", listOfInt, listOneTwoThreeBytes2, new(interface{}), interfacePtr([]*int32{int32Ptr(1), int32Ptr(2), int32Ptr(3)}), false, ""},
				{"list<int> nil elem", listOfInt, listOneTwoNullBytes2, new([]int), &[]int{1, 2, 0}, false, ""},
				{"list<int> nil elem pointers", listOfInt, listOneTwoNullBytes2, new([]*int), &[]*int{intPtr(1), intPtr(2), nil}, false, ""},
				{"list<int> nil elem []interface{}", listOfInt, listOneTwoNullBytes2, new([]interface{}), &[]interface{}{int32(1), int32(2), nil}, false, ""},
				{"list<int> nil elem interface{}", listOfInt, listOneTwoNullBytes2, new(interface{}), interfacePtr([]*int32{int32Ptr(1), int32Ptr(2), nil}), false, ""},
				{"list<int> pointer required", listOfInt, nil, []int{}, []int{}, true, fmt.Sprintf("cannot decode CQL %s as []int with %v: destination is not pointer", listOfInt.DataType(), version)},
				{"list<int> destination type not supported", listOfInt, nil, &map[string]int{}, new(map[string]int), true, fmt.Sprintf("cannot decode CQL %s as *map[string]int with %v: destination type not supported", listOfInt.DataType(), version)},
				{"list<set<text>> nil untyped", listOfSetOfVarchar, nil, nil, nil, true, "cannot decode CQL list<set<varchar>> as <nil> with ProtocolVersion OSS 2: destination is nil"},
				{"list<set<text>> nil slice", listOfSetOfVarchar, nil, new([][]string), new([][]string), true, ""},
				{"list<set<text>> empty", listOfSetOfVarchar, []byte{0, 0}, new([][]string), &[][]string{}, false, ""},
				{"list<set<text>> many elems", listOfSetOfVarchar, listAbcDefBytes2, new([][]string), &[][]string{{"abc"}, {"def"}}, false, ""},
				{"list<set<text>> array", listOfSetOfVarchar, listAbcDefBytes2, new([2][1]string), &[2][1]string{{"abc"}, {"def"}}, false, ""},
				{"list<set<text>> pointers", listOfSetOfVarchar, listAbcDefEmptyBytes2, new([][]*string), &[][]*string{{stringPtr("abc"), stringPtr("def")}, {}}, false, ""},
				{"list<set<text>> many elems [][]interface{}", listOfSetOfVarchar, listAbcDefEmptyBytes2, new([][]interface{}), &[][]interface{}{{"abc", "def"}, {}}, false, ""},
				{"list<set<text>> many elems interface{}", listOfSetOfVarchar, listAbcDefEmptyBytes2, new(interface{}), interfacePtr([][]*string{{stringPtr("abc"), stringPtr("def")}, {}}), false, ""},
				{"list<set<text>> nil elem", listOfSetOfVarchar, listAbcNullNullBytes2, new([][]string), &[][]string{{"abc", ""}, nil}, false, ""},
				{"list<set<text>> nil elem pointers", listOfSetOfVarchar, listAbcNullNullBytes2, new([][]*string), &[][]*string{{stringPtr("abc"), nil}, nil}, false, ""},
				{"list<set<text>> nil elem [][]interface{}", listOfSetOfVarchar, listAbcNullNullBytes2, new([][]interface{}), &[][]interface{}{{"abc", nil}, nil}, false, ""},
				{"list<set<text>> nil elem interface{}", listOfSetOfVarchar, listAbcNullNullBytes2, new(interface{}), interfacePtr([][]*string{{stringPtr("abc"), nil}, nil}), false, ""},
				{"list<set<text>> pointer required", listOfSetOfVarchar, nil, [][]string{}, [][]string{}, true, fmt.Sprintf("cannot decode CQL %s as [][]string with %s: destination is not pointer", listOfSetOfVarchar.DataType(), version)},
				{"list<set<text>> wrong destination type", listOfSetOfVarchar, nil, &map[string]string{}, new(map[string]string), true, fmt.Sprintf("cannot decode CQL %s as *map[string]string with %s: destination type not supported", listOfSetOfVarchar.DataType(), version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := tt.codec.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.want, tt.dest)
					assert.Equal(t, tt.wantNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_writeCollection(t *testing.T) {
	type args struct {
		ext          extractor
		elementCodec Codec
		size         int
		version      primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr string
	}{
		{
			"cannot write size",
			args{nil, nil, -1, primitive.ProtocolVersion5},
			nil,
			"cannot write collection size: expected collection size >= 0, got: -1",
		},
		{
			"cannot extract elem",
			args{func() extractor {
				ext := &mockExtractor{}
				ext.On("getElem", 0, 0).Return(nil, errSliceIndexOutOfRange("slice", 0))
				return ext
			}(), nil, 1, primitive.ProtocolVersion5},
			nil,
			"cannot extract element 0: slice index out of range: 0",
		},
		{
			"cannot encode",
			args{
				func() extractor {
					ext := &mockExtractor{}
					ext.On("getElem", 0, 0).Return(1, nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 1, primitive.ProtocolVersion5).Return(nil, errors.New("write failed"))
					return codec
				}(),
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode element 0: write failed",
		},
		{"success", args{
			func() extractor {
				ext := &mockExtractor{}
				ext.On("getElem", 0, 0).Return(1, nil)
				ext.On("getElem", 1, 1).Return(2, nil)
				ext.On("getElem", 2, 2).Return(3, nil)
				return ext
			}(),
			func() Codec {
				codec := &mockCodec{}
				codec.On("Encode", 1, primitive.ProtocolVersion5).Return([]byte{1}, nil)
				codec.On("Encode", 2, primitive.ProtocolVersion5).Return([]byte{2}, nil)
				codec.On("Encode", 3, primitive.ProtocolVersion5).Return([]byte{3}, nil)
				return codec
			}(),
			3,
			primitive.ProtocolVersion5,
		}, []byte{
			0, 0, 0, 3, // size
			0, 0, 0, 1, // elem 1
			1,
			0, 0, 0, 1, // elem 2
			2,
			0, 0, 0, 1, // elem 3
			3,
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := writeCollection(tt.args.ext, tt.args.elementCodec, tt.args.size, tt.args.version)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_readCollection(t *testing.T) {
	type args struct {
		source       []byte
		inj          func(int) (injector, error)
		elementCodec Codec
		version      primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			"cannot read size",
			args{[]byte{1}, nil, nil, primitive.ProtocolVersion5},
			"cannot read collection size: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create injector",
			args{
				[]byte{0, 0, 0, 1},
				func(int) (injector, error) { return nil, errors.New("cannot create injector") },
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot create injector",
		},
		{
			"cannot read element",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, // wrong [bytes]
				},
				func(int) (injector, error) { return &mockInjector{}, nil },
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot read element 0: cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create element",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // [bytes]
				},
				func(int) (injector, error) {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0).Return(nil, errors.New("wrong data type"))
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot create zero element 0: wrong data type",
		},
		{
			"cannot decode element",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // [bytes]
				},
				func(int) (injector, error) {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0).Return(new(int), nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Return(false, errors.New("decode failed"))
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot decode element 0: decode failed",
		},
		{
			"cannot set element",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // [bytes]
				},
				func(int) (injector, error) {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0).Return(new(int), nil)
					inj.On("setElem", 0, 0, intPtr(123), false, false).Return(errors.New("cannot set elem"))
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 123
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot inject element 0: cannot set elem",
		},
		{
			"bytes remaining",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // [bytes]
					1, // trailing bytes
				},
				func(int) (injector, error) {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0).Return(new(int), nil)
					inj.On("setElem", 0, 0, intPtr(123), false, false).Return(nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 123
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"source was not fully read: bytes total: 10, read: 9, remaining: 1",
		},
		{
			"success",
			args{
				[]byte{
					0, 0, 0, 3, // size
					0, 0, 0, 1, 1, // [bytes]
					0, 0, 0, 1, 2, // [bytes]
					0, 0, 0, 1, 3, // [bytes]
				},
				func(int) (injector, error) {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0).Return(new(int), nil)
					inj.On("zeroElem", 1, 1).Return(new(int), nil)
					inj.On("zeroElem", 2, 2).Return(new(int), nil)
					inj.On("setElem", 0, 0, intPtr(123), false, false).Return(nil)
					inj.On("setElem", 1, 1, intPtr(456), false, false).Return(nil)
					inj.On("setElem", 2, 2, intPtr(789), false, false).Return(nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 123
					}).Return(false, nil)
					codec.On("Decode", []byte{2}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 456
					}).Return(false, nil)
					codec.On("Decode", []byte{3}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 789
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := readCollection(tt.args.source, tt.args.inj, tt.args.elementCodec, tt.args.version)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_writeCollectionSize(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				size     int64
				wantDest []byte
				wantErr  string
			}{
				{"4 bytes zero", 0, []byte{0, 0, 0, 0}, ""},
				{"4 bytes max", math.MaxInt32, intMaxInt32Bytes, ""},
				{"4 bytes out of range neg", -1, nil, "expected collection size >= 0, got: -1"},
			}
			if strconv.IntSize == 64 {
				tests = append(tests, []struct {
					name     string
					size     int64
					wantDest []byte
					wantErr  string
				}{
					{"4 bytes out of range pos", math.MaxInt32 + 1, nil, "cannot write collection size: collection too large (2147483648 elements, max is 2147483647)"},
				}...)
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					gotErr := writeCollectionSize(int(tt.size), dest, version)
					assert.Equal(t, tt.wantDest, dest.Bytes())
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				size     int
				wantDest []byte
				wantErr  string
			}{
				{"2 bytes zero", 0, []byte{0, 0}, ""},
				{"2 bytes max", math.MaxUint16, encodeUint16(0xffff), ""},
				{"2 bytes out of range pos", math.MaxUint16 + 1, nil, "cannot write collection size: collection too large (65536 elements, max is 65535)"},
				{"2 bytes out of range neg", -1, nil, "expected collection size >= 0, got: -1"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					gotErr := writeCollectionSize(tt.size, dest, version)
					assert.Equal(t, tt.wantDest, dest.Bytes())
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
}

func Test_readCollectionSize(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				wantSize int
				wantErr  string
			}{
				{"4 bytes success", []byte{0, 0, 0, 3}, 3, ""},
				{"4 bytes error", []byte{0, 3}, 0, "cannot read collection size: cannot read [int]: unexpected EOF"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					gotSize, gotErr := readCollectionSize(bytes.NewReader(tt.source), version)
					assert.Equal(t, tt.wantSize, gotSize)
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				wantSize int
				wantErr  string
			}{
				{"2 bytes success", []byte{0, 3}, 3, ""},
				{"2 bytes error", []byte{3}, 0, "cannot read collection size: cannot read [short]: unexpected EOF"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					gotSize, gotErr := readCollectionSize(bytes.NewReader(tt.source), version)
					assert.Equal(t, tt.wantSize, gotSize)
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
}
