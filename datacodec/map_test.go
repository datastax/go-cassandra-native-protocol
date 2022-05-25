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
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestNewMap(t *testing.T) {
	tests := []struct {
		name     string
		dataType *datatype.Map
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
			datatype.NewMap(datatype.Int, datatype.Varchar),
			&mapCodec{
				dataType:   datatype.NewMap(datatype.Int, datatype.Varchar),
				keyCodec:   &intCodec{},
				valueCodec: &stringCodec{dataType: datatype.Varchar},
			},
			"",
		},
		{
			"complex",
			datatype.NewMap(datatype.Int, datatype.NewMap(datatype.Int, datatype.Varchar)),
			&mapCodec{
				dataType: datatype.NewMap(datatype.Int, datatype.NewMap(datatype.Int, datatype.Varchar)),
				keyCodec: &intCodec{},
				valueCodec: &mapCodec{
					dataType:   datatype.NewMap(datatype.Int, datatype.Varchar),
					keyCodec:   &intCodec{},
					valueCodec: &stringCodec{dataType: datatype.Varchar},
				},
			},
			"",
		},
		{
			"wrong key type",
			datatype.NewMap(wrongDataType{}, datatype.Int),
			nil,
			"cannot create codec for map keys: cannot create data codec for CQL type 666",
		},
		{
			"wrong value type",
			datatype.NewMap(datatype.Int, wrongDataType{}),
			nil,
			"cannot create codec for map values: cannot create data codec for CQL type 666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := NewMap(tt.dataType)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

var (
	mapSimple, _      = NewMap(datatype.NewMap(datatype.Int, datatype.Varchar))
	mapComplex, _     = NewMap(datatype.NewMap(datatype.Int, datatype.NewMap(datatype.Int, datatype.Varchar)))
	mapCoordinates, _ = NewMap(datatype.NewMap(datatype.Varchar, datatype.Float))
)

type coordinates struct {
	X float32
	Y float32 `cassandra:"y"`
}

var (
	mapOneTwoAbcBytes2 = []byte{
		0, 1,
		0, 0, 0, 4,
		0, 0, 0, 12,
		0, 0, 0, 3,
		a, b, c,
	}
	mapOneTwoAbcBytes4 = []byte{
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 12,
		0, 0, 0, 3,
		a, b, c,
	}
	mapZeroOneTwoAbcBytes2 = []byte{
		0, 1, // length of outer collection
		0, 0, 0, 4, // length of outer collection 1st key
		0, 0, 0, 0, // 1st key
		0, 0, 0, 17, // length of outer collection 1st value
		0, 1, // length of 1st inner collection
		0, 0, 0, 4, // length of 1st inner collection 1st key
		0, 0, 0, 12, // 1st inner collection 1st key
		0, 0, 0, 3, // length of 1st inner collection 1st value
		a, b, c, // 1st inner collection 1st value
	}
	mapZeroOneTwoAbcBytes4 = []byte{
		0, 0, 0, 1, // length of outer collection
		0, 0, 0, 4, // length of outer collection 1st key
		0, 0, 0, 0, // 1st key
		0, 0, 0, 19, // length of outer collection 1st value
		0, 0, 0, 1, // length of 1st inner collection
		0, 0, 0, 4, // length of 1st inner collection 1st key
		0, 0, 0, 12, // 1st inner collection 1st key
		0, 0, 0, 3, // length of 1st inner collection 1st value
		a, b, c, // 1st inner collection 1st value
	}
	mapCoordinatesBytes4 = []byte{
		0, 0, 0, 2,
		0, 0, 0, 1,
		x,
		0, 0, 0, 4,
		0x41, 0x45, 0x70, 0xa4,
		0, 0, 0, 1,
		y,
		0, 0, 0, 4,
		0xc2, 0x63, 0x1e, 0xb8,
	}
	mapCoordinatesEmptyBytes4 = []byte{
		0, 0, 0, 2,
		0, 0, 0, 1,
		x,
		0, 0, 0, 4,
		0, 0, 0, 0,
		0, 0, 0, 1,
		y,
		0, 0, 0, 4,
		0, 0, 0, 0,
	}
	mapCoordinatesBytes2 = []byte{
		0, 2,
		0, 0, 0, 1,
		x,
		0, 0, 0, 4,
		0x41, 0x45, 0x70, 0xa4,
		0, 0, 0, 1,
		y,
		0, 0, 0, 4,
		0xc2, 0x63, 0x1e, 0xb8,
	}
	mapCoordinatesEmptyBytes2 = []byte{
		0, 2,
		0, 0, 0, 1,
		x,
		0, 0, 0, 4,
		0, 0, 0, 0,
		0, 0, 0, 1,
		y,
		0, 0, 0, 4,
		0, 0, 0, 0,
	}
	mapNullAbcBytes2 = []byte{
		0, 1,
		255, 255, 255, 255,
		0, 0, 0, 3,
		a, b, c,
	}
	mapNullAbcBytes4 = []byte{
		0, 0, 0, 1,
		255, 255, 255, 255,
		0, 0, 0, 3,
		a, b, c,
	}
	mapOneTwoNullBytes2 = []byte{
		0, 1,
		0, 0, 0, 4,
		0, 0, 0, 12,
		255, 255, 255, 255,
	}
	mapOneTwoNullBytes4 = []byte{
		0, 0, 0, 1,
		0, 0, 0, 4,
		0, 0, 0, 12,
		255, 255, 255, 255,
	}
)

func Test_mapCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   interface{}
				expected []byte
				err      string
			}{
				{"map<int,text> nil untyped", mapSimple, nil, nil, ""},
				{"map<int,text> nil slice", mapSimple, new(map[int]string), nil, ""},
				{"map<int,text> empty", mapSimple, map[int]string{}, []byte{0, 0, 0, 0}, ""},
				{"map<int,text> one elem", mapSimple, map[int]string{12: "abc"}, mapOneTwoAbcBytes4, ""},
				{"map<int,text> non-empty", mapSimple, map[int]string{12: "abc"}, mapOneTwoAbcBytes4, ""},
				{"map<int,text> map pointer", mapSimple, &map[int]string{12: "abc"}, mapOneTwoAbcBytes4, ""},
				{"map<int,text> non-empty elems pointers", mapSimple, map[*int]*string{intPtr(12): stringPtr("abc")}, mapOneTwoAbcBytes4, ""},
				{"map<int,text> non-empty map pointer elems pointers", mapSimple, &map[*int]*string{intPtr(12): stringPtr("abc")}, mapOneTwoAbcBytes4, ""},
				{"map<int,text> non-empty interface{}", mapSimple, map[int]interface{}{12: "abc"}, mapOneTwoAbcBytes4, ""},
				{"map<int,text> nil key", mapSimple, map[interface{}]interface{}{nil: "abc"}, []byte{0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x3, a, b, c}, ""},
				{"map<int,text> nil value", mapSimple, map[int]interface{}{12: nil}, []byte{0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0xc, 0xff, 0xff, 0xff, 0xff}, ""},
				{"map<int,text> wrong source type", mapSimple, 123, nil, fmt.Sprintf("cannot encode int as CQL %s with %s: source type not supported", mapSimple.DataType(), version)},
				{"map<int,text> wrong source type nil", mapSimple, []int(nil), nil, fmt.Sprintf("cannot encode []int as CQL %s with %s: source type not supported", mapSimple.DataType(), version)},
				{"map<int,text> wrong source type nil pointer", mapSimple, new([]int), nil, fmt.Sprintf("cannot encode *[]int as CQL %s with %s: source type not supported", mapSimple.DataType(), version)},
				{"map<int,map<int,varchar>> nil untyped", mapComplex, nil, nil, ""},
				{"map<int,map<int,varchar>> nil slice", mapComplex, map[int]map[int]string(nil), nil, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, map[int]map[int]string{}, []byte{0, 0, 0, 0}, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, map[int]map[int]string{0: {12: "abc"}}, mapZeroOneTwoAbcBytes4, ""},
				{"map<int,map<int,varchar>> non-empty pointer", mapComplex, &map[int]map[int]string{0: {12: "abc"}}, mapZeroOneTwoAbcBytes4, ""},
				{"map<int,map<int,varchar>> non-empty pointer elem pointers", mapComplex, &map[*int]map[*int]*string{intPtr(0): {intPtr(12): stringPtr("abc")}}, mapZeroOneTwoAbcBytes4, ""},
				{"map<int,map<int,varchar>> interface", mapComplex, &map[interface{}]map[interface{}]interface{}{intPtr(0): {intPtr(12): stringPtr("abc")}}, mapZeroOneTwoAbcBytes4, ""},
				{"coordinates empty", mapCoordinates, &coordinates{}, mapCoordinatesEmptyBytes4, ""},
				{"coordinates non empty", mapCoordinates, &coordinates{X: 12.34, Y: -56.78}, mapCoordinatesBytes4, ""},
				{"coordinates wrong key", mapSimple, &coordinates{X: 12.34, Y: -56.78}, nil, fmt.Sprintf("cannot encode *datacodec.coordinates as CQL map<int,varchar> with %v: wrong map key, expected varchar or ascii, got: int", version)},
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
				{"map<int,text> nil untyped", mapSimple, nil, nil, ""},
				{"map<int,text> nil slice", mapSimple, new(map[int]string), nil, ""},
				{"map<int,text> empty", mapSimple, map[int]string{}, []byte{0, 0}, ""},
				{"map<int,text> non-empty", mapSimple, map[int]string{12: "abc"}, mapOneTwoAbcBytes2, ""},
				{"map<int,text> non-empty pointer", mapSimple, &map[int]string{12: "abc"}, mapOneTwoAbcBytes2, ""},
				{"map<int,text> non-empty elems pointers", mapSimple, map[*int]*string{intPtr(12): stringPtr("abc")}, mapOneTwoAbcBytes2, ""},
				{"map<int,text> non-empty map pointer elems pointers", mapSimple, &map[*int]*string{intPtr(12): stringPtr("abc")}, mapOneTwoAbcBytes2, ""},
				{"map<int,text> non-empty interface{}", mapSimple, map[int]interface{}{12: "abc"}, mapOneTwoAbcBytes2, ""},
				{"map<int,text> nil key", mapSimple, map[interface{}]interface{}{nil: "abc"}, []byte{0x0, 0x1, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x3, a, b, c}, ""},
				{"map<int,text> nil value", mapSimple, map[int]interface{}{12: nil}, []byte{0x0, 0x1, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0xc, 0xff, 0xff, 0xff, 0xff}, ""},
				{"map<int,text> wrong source type", mapSimple, 123, nil, fmt.Sprintf("cannot encode int as CQL %s with %s: source type not supported", mapSimple.DataType(), version)},
				{"map<int,text> wrong source type nil", mapSimple, []int(nil), nil, fmt.Sprintf("cannot encode []int as CQL %s with %s: source type not supported", mapSimple.DataType(), version)},
				{"map<int,text> wrong source type nil pointer", mapSimple, new([]int), nil, fmt.Sprintf("cannot encode *[]int as CQL %s with %s: source type not supported", mapSimple.DataType(), version)},
				{"map<int,map<int,varchar>> nil untyped", mapComplex, nil, nil, ""},
				{"map<int,map<int,varchar>> nil slice", mapComplex, map[int]map[int]string(nil), nil, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, map[int]map[int]string{}, []byte{0, 0}, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, map[int]map[int]string{0: {12: "abc"}}, mapZeroOneTwoAbcBytes2, ""},
				{"map<int,map<int,varchar>> non-empty pointer", mapComplex, &map[int]map[int]string{0: {12: "abc"}}, mapZeroOneTwoAbcBytes2, ""},
				{"map<int,map<int,varchar>> non-empty pointer elem pointers", mapComplex, &map[*int]map[*int]*string{intPtr(0): {intPtr(12): stringPtr("abc")}}, mapZeroOneTwoAbcBytes2, ""},
				{"map<int,map<int,varchar>> interface", mapComplex, &map[interface{}]map[interface{}]interface{}{intPtr(0): {intPtr(12): stringPtr("abc")}}, mapZeroOneTwoAbcBytes2, ""},
				{"coordinates empty", mapCoordinates, &coordinates{}, mapCoordinatesEmptyBytes2, ""},
				{"coordinates non empty", mapCoordinates, &coordinates{X: 12.34, Y: -56.78}, mapCoordinatesBytes2, ""},
				{"coordinates wrong key", mapSimple, &coordinates{X: 12.34, Y: -56.78}, nil, fmt.Sprintf("cannot encode *datacodec.coordinates as CQL map<int,varchar> with %v: wrong map key, expected varchar or ascii, got: int", version)},
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

func Test_mapCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThan(primitive.ProtocolVersion3) {
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
				{"map<int,text> nil untyped", mapSimple, nil, nil, nil, true, fmt.Sprintf("cannot decode CQL map<int,varchar> as <nil> with %s: destination is nil", version)},
				{"map<int,text> nil slice", mapSimple, nil, new(map[int]string), new(map[int]string), true, ""},
				{"map<int,text> empty", mapSimple, []byte{0, 0, 0, 0}, new(map[int]string), &map[int]string{}, false, ""},
				{"map<int,text> non-empty", mapSimple, mapOneTwoAbcBytes4, new(map[int]string), &map[int]string{12: "abc"}, false, ""},
				{"map<int,text> non-empty pointers", mapSimple, mapOneTwoAbcBytes4, new(map[int]*string), &map[int]*string{12: stringPtr("abc")}, false, ""},
				{"map<int,text> non-empty map[interface]", mapSimple, mapOneTwoAbcBytes4, new(map[interface{}]interface{}), &map[interface{}]interface{}{int32(12): "abc"}, false, ""},
				{"map<int,text> pointer required", mapSimple, nil, map[int]string{}, map[int]string{}, true, fmt.Sprintf("cannot decode CQL %s as map[int]string with %v: destination is not pointer", mapSimple.DataType(), version)},
				{"map<int,text> destination type not supported", mapSimple, nil, new([]int), new([]int), true, fmt.Sprintf("cannot decode CQL %s as *[]int with %v: destination type not supported", mapSimple.DataType(), version)},
				{"map<int,map<int,varchar>> nil untyped", mapComplex, nil, nil, nil, true, fmt.Sprintf("cannot decode CQL map<int,map<int,varchar>> as <nil> with %s: destination is nil", version)},
				{"map<int,map<int,varchar>> nil slice", mapComplex, nil, new(map[int]map[int]string), new(map[int]map[int]string), true, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, []byte{0, 0, 0, 0}, new(map[int]map[int]string), &map[int]map[int]string{}, false, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, mapZeroOneTwoAbcBytes4, new(map[int]map[int]string), &map[int]map[int]string{0: {12: "abc"}}, false, ""},
				{"map<int,map<int,varchar>> non-empty pointers", mapComplex, mapZeroOneTwoAbcBytes4, new(map[int]map[int]*string), &map[int]map[int]*string{0: {12: stringPtr("abc")}}, false, ""},
				{"map<int,map<int,varchar>> non-empty map[interface]", mapComplex, mapZeroOneTwoAbcBytes4, new(map[interface{}]map[interface{}]interface{}), &map[interface{}]map[interface{}]interface{}{int32(0): {int32(12): "abc"}}, false, ""},
				{"map<int,map<int,varchar>> pointer required", mapComplex, nil, map[int]map[int]string{}, map[int]map[int]string{}, true, fmt.Sprintf("cannot decode CQL %s as map[int]map[int]string with %s: destination is not pointer", mapComplex.DataType(), version)},
				{"map<int,map<int,varchar>> wrong destination type", mapComplex, nil, new([]string), new([]string), true, fmt.Sprintf("cannot decode CQL %s as *[]string with %s: destination type not supported", mapComplex.DataType(), version)},
				{"coordinates nil", mapCoordinates, nil, &coordinates{}, &coordinates{}, true, ""},
				{"coordinates empty", mapCoordinates, mapCoordinatesEmptyBytes4, &coordinates{}, &coordinates{}, false, ""},
				{"coordinates non empty", mapCoordinates, mapCoordinatesBytes4, &coordinates{}, &coordinates{X: 12.34, Y: -56.78}, false, ""},
				{"coordinates wrong", mapSimple, mapCoordinatesBytes4, &coordinates{}, &coordinates{}, false, fmt.Sprintf("cannot decode CQL map<int,varchar> as *datacodec.coordinates with %v: wrong map key, expected varchar or ascii, got: int", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := tt.codec.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.want, tt.dest)
					assert.Equal(t, tt.wantNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
			testsNull := []struct {
				name      string
				source    []byte
				wantKey   *int32
				wantValue *string
			}{
				{"map<int,text> nil key", mapNullAbcBytes4, nil, stringPtr("abc")},
				{"map<int,text> nil value", mapOneTwoNullBytes4, int32Ptr(12), nil},
				{"map<int,text> non nil", mapOneTwoAbcBytes4, int32Ptr(12), stringPtr("abc")},
			}
			for _, tt := range testsNull {
				t.Run(tt.name, func(t *testing.T) {
					var dest interface{}
					wasNull, err := mapSimple.Decode(tt.source, &dest, version)
					assert.NoError(t, err)
					assert.Len(t, dest, 1)
					assert.IsType(t, map[*int32]*string{}, dest)
					for k, v := range dest.(map[*int32]*string) {
						assert.Equal(t, tt.wantKey, k)
						assert.Equal(t, tt.wantValue, v)
					}
					assert.False(t, wasNull)
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
				{"map<int,text> nil untyped", mapSimple, nil, nil, nil, true, fmt.Sprintf("cannot decode CQL map<int,varchar> as <nil> with %s: destination is nil", version)},
				{"map<int,text> nil slice", mapSimple, nil, new(map[int]string), new(map[int]string), true, ""},
				{"map<int,text> empty", mapSimple, []byte{0, 0}, new(map[int]string), &map[int]string{}, false, ""},
				{"map<int,text> non-empty", mapSimple, mapOneTwoAbcBytes2, new(map[int]string), &map[int]string{12: "abc"}, false, ""},
				{"map<int,text> non-empty pointers", mapSimple, mapOneTwoAbcBytes2, new(map[int]*string), &map[int]*string{12: stringPtr("abc")}, false, ""},
				{"map<int,text> non-empty map[interface]", mapSimple, mapOneTwoAbcBytes2, new(map[interface{}]interface{}), &map[interface{}]interface{}{int32(12): "abc"}, false, ""},
				{"map<int,text> pointer required", mapSimple, nil, map[int]string{}, map[int]string{}, true, fmt.Sprintf("cannot decode CQL %s as map[int]string with %v: destination is not pointer", mapSimple.DataType(), version)},
				{"map<int,text> destination type not supported", mapSimple, nil, new([]int), new([]int), true, fmt.Sprintf("cannot decode CQL %s as *[]int with %v: destination type not supported", mapSimple.DataType(), version)},
				{"map<int,map<int,varchar>> nil untyped", mapComplex, nil, nil, nil, true, fmt.Sprintf("cannot decode CQL map<int,map<int,varchar>> as <nil> with %s: destination is nil", version)},
				{"map<int,map<int,varchar>> nil slice", mapComplex, nil, new(map[int]map[int]string), new(map[int]map[int]string), true, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, []byte{0, 0}, new(map[int]map[int]string), &map[int]map[int]string{}, false, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, mapZeroOneTwoAbcBytes2, new(map[int]map[int]string), &map[int]map[int]string{0: {12: "abc"}}, false, ""},
				{"map<int,map<int,varchar>> non-empty pointers", mapComplex, mapZeroOneTwoAbcBytes2, new(map[int]map[int]*string), &map[int]map[int]*string{0: {12: stringPtr("abc")}}, false, ""},
				{"map<int,map<int,varchar>> non-empty map[interface]", mapComplex, mapZeroOneTwoAbcBytes2, new(map[interface{}]map[interface{}]interface{}), &map[interface{}]map[interface{}]interface{}{int32(0): {int32(12): "abc"}}, false, ""},
				{"map<int,map<int,varchar>> pointer required", mapComplex, nil, map[int]map[int]string{}, map[int]map[int]string{}, true, fmt.Sprintf("cannot decode CQL %s as map[int]map[int]string with %s: destination is not pointer", mapComplex.DataType(), version)},
				{"map<int,map<int,varchar>> wrong destination type", mapComplex, nil, new([]string), new([]string), true, fmt.Sprintf("cannot decode CQL %s as *[]string with %s: destination type not supported", mapComplex.DataType(), version)},
				{"coordinates nil", mapCoordinates, nil, &coordinates{}, &coordinates{}, true, ""},
				{"coordinates empty", mapCoordinates, mapCoordinatesEmptyBytes2, &coordinates{}, &coordinates{}, false, ""},
				{"coordinates non empty", mapCoordinates, mapCoordinatesBytes2, &coordinates{}, &coordinates{X: 12.34, Y: -56.78}, false, ""},
				{"coordinates wrong", mapSimple, mapCoordinatesBytes2, &coordinates{}, &coordinates{}, false, fmt.Sprintf("cannot decode CQL map<int,varchar> as *datacodec.coordinates with %v: wrong map key, expected varchar or ascii, got: int", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := tt.codec.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.want, tt.dest)
					assert.Equal(t, tt.wantNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
			testsNull := []struct {
				name      string
				source    []byte
				wantKey   *int32
				wantValue *string
			}{
				{"map<int,text> nil key", mapNullAbcBytes2, nil, stringPtr("abc")},
				{"map<int,text> nil value", mapOneTwoNullBytes2, int32Ptr(12), nil},
				{"map<int,text> non nil", mapOneTwoAbcBytes2, int32Ptr(12), stringPtr("abc")},
			}
			for _, tt := range testsNull {
				t.Run(tt.name, func(t *testing.T) {
					var dest interface{}
					wasNull, err := mapSimple.Decode(tt.source, &dest, version)
					assert.NoError(t, err)
					assert.Len(t, dest, 1)
					assert.IsType(t, map[*int32]*string{}, dest)
					for k, v := range dest.(map[*int32]*string) {
						assert.Equal(t, tt.wantKey, k)
						assert.Equal(t, tt.wantValue, v)
					}
					assert.False(t, wasNull)
				})
			}
		})
	}
}

func Test_writeMap(t *testing.T) {
	type args struct {
		ext        keyValueExtractor
		keyCodec   Codec
		valueCodec Codec
		size       int
		version    primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr string
	}{
		{
			"cannot write size",
			args{nil, nil, nil, -1, primitive.ProtocolVersion5},
			nil,
			"cannot write collection size: expected collection size >= 0, got: -1",
		},
		{
			"cannot extract value",
			args{func() keyValueExtractor {
				ext := &mockKeyValueExtractor{}
				ext.On("getKey", 0).Return(123, nil)
				ext.On("getElem", 0, 123).Return(nil, errors.New("cannot extract elem"))
				return ext
			}(), nil, nil, 1, primitive.ProtocolVersion5},
			nil,
			"cannot extract entry 0 value: cannot extract elem",
		},
		{
			"cannot encode key",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(123, nil)
					ext.On("getElem", 0, 123).Return("abc", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return(nil, errors.New("write key failed"))
					return codec
				}(),
				nil,
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode entry 0 key: write key failed",
		},
		{
			"cannot encode value",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(123, nil)
					ext.On("getElem", 0, 123).Return("abc", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return([]byte{1}, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", "abc", primitive.ProtocolVersion5).Return(nil, errors.New("write value failed"))
					return codec
				}(),
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode entry 0 value: write value failed",
		},
		{"success",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(12, nil)
					ext.On("getElem", 0, 12).Return("abc", nil)
					ext.On("getKey", 1).Return(34, nil)
					ext.On("getElem", 1, 34).Return("def", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 12, primitive.ProtocolVersion5).Return([]byte{12}, nil)
					codec.On("Encode", 34, primitive.ProtocolVersion5).Return([]byte{34}, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", "abc", primitive.ProtocolVersion5).Return([]byte{a, b, c}, nil)
					codec.On("Encode", "def", primitive.ProtocolVersion5).Return([]byte{d, e, f}, nil)
					return codec
				}(),
				2,
				primitive.ProtocolVersion5,
			},
			[]byte{
				0, 0, 0, 2, // size
				0, 0, 0, 1, // elem 1 key
				12,
				0, 0, 0, 3, // elem 1 value
				a, b, c,
				0, 0, 0, 1, // elem 2 key
				34,
				0, 0, 0, 3, // elem 2 value
				d, e, f,
			}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := writeMap(tt.args.ext, tt.args.size, tt.args.keyCodec, tt.args.valueCodec, tt.args.version)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_readMap(t *testing.T) {
	type args struct {
		source     []byte
		inj        func(int) (keyValueInjector, error)
		keyCodec   Codec
		valueCodec Codec
		version    primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			"cannot read size",
			args{[]byte{1}, nil, nil, nil, primitive.ProtocolVersion5},
			"cannot read collection size: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create injector",
			args{
				[]byte{0, 0, 0, 1},
				func(int) (keyValueInjector, error) { return nil, errors.New("cannot create injector") },
				nil,
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot create injector",
		},
		{
			"cannot read key",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, // wrong [bytes]
				},
				func(int) (keyValueInjector, error) { return &mockKeyValueInjector{}, nil },
				nil,
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot read entry 0 key: cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot read value",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, // wrong [bytes]
				},
				func(int) (keyValueInjector, error) { return &mockKeyValueInjector{}, nil },
				nil,
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot read entry 0 value: cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create key",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 1, // value
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(nil, errors.New("wrong data type"))
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					return codec
				}(),
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot create zero entry 0 key: wrong data type",
		},
		{
			"cannot decode key",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 1, // value
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(new(int), nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Return(false, errors.New("decode key failed"))
					return codec
				}(),
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot decode entry 0 key: decode key failed",
		},
		{
			"cannot create value",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 1, // value
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12)).Return(nil, errors.New("wrong data type"))
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot create zero entry 0 value: wrong data type",
		},
		{
			"cannot decode value",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 2, // value
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12)).Return(new(string), nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Return(false, errors.New("decode value failed"))
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot decode entry 0 value: decode value failed",
		},
		{
			"cannot set element",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 2, // value
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12)).Return(new(string), nil)
					inj.On("setElem", 0, intPtr(12), stringPtr("abc"), false, false).Return(errors.New("cannot set elem"))
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot inject entry 0: cannot set elem",
		},
		{
			"bytes remaining",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 2, // value
					1, // trailing
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12)).Return(new(string), nil)
					inj.On("setElem", 0, intPtr(12), stringPtr("abc"), false, false).Return(nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"source was not fully read: bytes total: 15, read: 14, remaining: 1",
		},
		{
			"success",
			args{
				[]byte{
					0, 0, 0, 2, // size
					0, 0, 0, 1, 1, // key1
					0, 0, 0, 1, 2, // value1
					0, 0, 0, 1, 3, // key2
					0, 0, 0, 1, 4, // value2
				},
				func(int) (keyValueInjector, error) {
					inj := &mockKeyValueInjector{}
					inj.On("zeroKey", 0).Return(new(int), nil)
					inj.On("zeroKey", 1).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12)).Return(new(string), nil)
					inj.On("zeroElem", 1, intPtr(34)).Return(new(string), nil)
					inj.On("setElem", 0, intPtr(12), stringPtr("abc"), false, false).Return(nil)
					inj.On("setElem", 1, intPtr(34), stringPtr("def"), false, false).Return(nil)
					return inj, nil
				},
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					// the call should be with
					codec.On("Decode", []byte{3}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 34
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					codec.On("Decode", []byte{4}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "def"
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
			gotErr := readMap(tt.args.source, tt.args.inj, tt.args.keyCodec, tt.args.valueCodec, tt.args.version)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
