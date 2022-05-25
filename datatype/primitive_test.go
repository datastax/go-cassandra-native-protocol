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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrimitiveType(t *testing.T) {
	tests := []struct {
		name     string
		input    *PrimitiveType
		expected primitive.DataTypeCode
	}{
		{"Ascii", Ascii, primitive.DataTypeCodeAscii},
		{"Bigint", Bigint, primitive.DataTypeCodeBigint},
		{"Blob", Blob, primitive.DataTypeCodeBlob},
		{"Boolean", Boolean, primitive.DataTypeCodeBoolean},
		{"Counter", Counter, primitive.DataTypeCodeCounter},
		{"Decimal", Decimal, primitive.DataTypeCodeDecimal},
		{"Double", Double, primitive.DataTypeCodeDouble},
		{"Float", Float, primitive.DataTypeCodeFloat},
		{"Int", Int, primitive.DataTypeCodeInt},
		{"Timestamp", Timestamp, primitive.DataTypeCodeTimestamp},
		{"Uuid", Uuid, primitive.DataTypeCodeUuid},
		{"Varchar", Varchar, primitive.DataTypeCodeVarchar},
		{"Varint", Varint, primitive.DataTypeCodeVarint},
		{"Timeuuid", Timeuuid, primitive.DataTypeCodeTimeuuid},
		{"Inet", Inet, primitive.DataTypeCodeInet},
		{"Date", Date, primitive.DataTypeCodeDate},
		{"Time", Time, primitive.DataTypeCodeTime},
		{"Smallint", Smallint, primitive.DataTypeCodeSmallint},
		{"Tinyint", Tinyint, primitive.DataTypeCodeTinyint},
		{"Duration", Duration, primitive.DataTypeCodeDuration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input.Code()
			assert.Equal(t, test.expected, actual)
		})
	}
}
