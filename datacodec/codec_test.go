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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
)

func TestNewCodec(t *testing.T) {
	customType := datatype.NewCustom("com.example.Type")
	listType := datatype.NewList(datatype.Int)
	listCodec, _ := NewList(listType)
	setType := datatype.NewSet(datatype.Int)
	setCodec, _ := NewSet(setType)
	mapType := datatype.NewMap(datatype.Int, datatype.Varchar)
	mapCodec, _ := NewMap(mapType)
	tupleType := datatype.NewTuple(datatype.Int)
	tupleCodec, _ := NewTuple(tupleType)
	userDefinedType, _ := datatype.NewUserDefined("ks1", "table1", []string{"f1"}, []datatype.DataType{datatype.Int})
	userDefinedCodec, _ := NewUserDefined(userDefinedType)
	tests := []struct {
		name      string
		dt        datatype.DataType
		wantCodec Codec
		wantErr   string
	}{
		{"Ascii", datatype.Ascii, Ascii, ""},
		{"Bigint", datatype.Bigint, Bigint, ""},
		{"Blob", datatype.Blob, Blob, ""},
		{"Boolean", datatype.Boolean, Boolean, ""},
		{"Counter", datatype.Counter, Counter, ""},
		{"Custom", customType, NewCustom(customType), ""},
		{"Date", datatype.Date, Date, ""},
		{"Decimal", datatype.Decimal, Decimal, ""},
		{"Double", datatype.Double, Double, ""},
		{"Duration", datatype.Duration, Duration, ""},
		{"Float", datatype.Float, Float, ""},
		{"Inet", datatype.Inet, Inet, ""},
		{"Int", datatype.Int, Int, ""},
		{"Smallint", datatype.Smallint, Smallint, ""},
		{"Time", datatype.Time, Time, ""},
		{"Timestamp", datatype.Timestamp, Timestamp, ""},
		{"Timeuuid", datatype.Timeuuid, Timeuuid, ""},
		{"Tinyint", datatype.Tinyint, Tinyint, ""},
		{"Uuid", datatype.Uuid, Uuid, ""},
		{"Varchar", datatype.Varchar, Varchar, ""},
		{"Varint", datatype.Varint, Varint, ""},
		{"List", listType, listCodec, ""},
		{"Set", setType, setCodec, ""},
		{"Map", mapType, mapCodec, ""},
		{"Tuple", tupleType, tupleCodec, ""},
		{"UserDefined", userDefinedType, userDefinedCodec, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCodec, gotErr := NewCodec(tt.dt)
			assert.Equal(t, tt.wantCodec, gotCodec)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func TestPreferredGoType(t *testing.T) {
	customType := datatype.NewCustom("com.example.Type")
	listType := datatype.NewList(datatype.Int)
	setType := datatype.NewSet(datatype.Int)
	mapType := datatype.NewMap(datatype.Int, datatype.Varchar)
	tupleType := datatype.NewTuple(datatype.Int)
	userDefinedType, _ := datatype.NewUserDefined("ks1", "table1", []string{"f1"}, []datatype.DataType{datatype.Int})
	tests := []struct {
		name     string
		dt       datatype.DataType
		wantType reflect.Type
		wantErr  string
	}{
		{"Ascii", datatype.Ascii, typeOfString, ""},
		{"Bigint", datatype.Bigint, typeOfInt64, ""},
		{"Blob", datatype.Blob, typeOfByteSlice, ""},
		{"Boolean", datatype.Boolean, typeOfBoolean, ""},
		{"Counter", datatype.Counter, typeOfInt64, ""},
		{"Custom", customType, typeOfByteSlice, ""},
		{"Date", datatype.Date, typeOfTime, ""},
		{"Decimal", datatype.Decimal, typeOfCqlDecimal, ""},
		{"Double", datatype.Double, typeOfFloat64, ""},
		{"Duration", datatype.Duration, typeOfCqlDuration, ""},
		{"Float", datatype.Float, typeOfFloat32, ""},
		{"Inet", datatype.Inet, typeOfNetIP, ""},
		{"Int", datatype.Int, typeOfInt32, ""},
		{"Smallint", datatype.Smallint, typeOfInt16, ""},
		{"Time", datatype.Time, typeOfDuration, ""},
		{"Timestamp", datatype.Timestamp, typeOfTime, ""},
		{"Timeuuid", datatype.Timeuuid, typeOfUUID, ""},
		{"Tinyint", datatype.Tinyint, typeOfInt8, ""},
		{"Uuid", datatype.Uuid, typeOfUUID, ""},
		{"Varchar", datatype.Varchar, typeOfString, ""},
		{"Varint", datatype.Varint, typeOfBigIntPointer, ""},
		{"List", listType, reflect.TypeOf([]*int32{}), ""},
		{"Set", setType, reflect.TypeOf([]*int32{}), ""},
		{"Map", mapType, reflect.TypeOf(map[*int32]*string{}), ""},
		{"Tuple", tupleType, reflect.TypeOf([]interface{}{}), ""},
		{"UserDefined", userDefinedType, reflect.TypeOf(map[string]interface{}{}), ""},
		{"List wrong", datatype.NewList(wrongDataType{}), nil, "could not find any suitable Go type for CQL type 666"},
		{"Set wrong", datatype.NewSet(wrongDataType{}), nil, "could not find any suitable Go type for CQL type 666"},
		{"Map wrong key", datatype.NewMap(wrongDataType{}, datatype.Int), nil, "could not find any suitable Go type for CQL type 666"},
		{"Map wrong value", datatype.NewMap(datatype.Int, wrongDataType{}), nil, "could not find any suitable Go type for CQL type 666"},
		{"wrong", wrongDataType{}, nil, "could not find any suitable Go type for CQL type 666"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotErr := PreferredGoType(tt.dt)
			assert.Equal(t, tt.wantType, gotType, "expected %s, got %s", tt.wantType, gotType)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
