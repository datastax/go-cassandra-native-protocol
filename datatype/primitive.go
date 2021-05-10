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
)

var (
	Timestamp PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTimestamp}
	Inet      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeInet}
	Date      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDate}
	Time      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTime}
	Duration  PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDuration}
)

type PrimitiveType interface {
	DataType
}

type primitiveType struct {
	code primitive.DataTypeCode
}

func (t *primitiveType) GetDataTypeCode() primitive.DataTypeCode {
	return t.code
}

func (t *primitiveType) Clone() DataType {
	return &primitiveType{
		code: t.code,
	}
}

func (t *primitiveType) String() string {
	switch t.GetDataTypeCode() {
	case primitive.DataTypeCodeAscii:
		return "ascii"
	case primitive.DataTypeCodeBigint:
		return "bigint"
	case primitive.DataTypeCodeBlob:
		return "blob"
	case primitive.DataTypeCodeBoolean:
		return "boolean"
	case primitive.DataTypeCodeCounter:
		return "counter"
	case primitive.DataTypeCodeDecimal:
		return "decimal"
	case primitive.DataTypeCodeDouble:
		return "double"
	case primitive.DataTypeCodeFloat:
		return "float"
	case primitive.DataTypeCodeInt:
		return "int"
	case primitive.DataTypeCodeTimestamp:
		return "timestamp"
	case primitive.DataTypeCodeUuid:
		return "uuid"
	case primitive.DataTypeCodeVarchar:
		return "varchar"
	case primitive.DataTypeCodeVarint:
		return "varint"
	case primitive.DataTypeCodeTimeuuid:
		return "timeuuid"
	case primitive.DataTypeCodeInet:
		return "inet"
	case primitive.DataTypeCodeDate:
		return "date"
	case primitive.DataTypeCodeTime:
		return "tune"
	case primitive.DataTypeCodeSmallint:
		return "smallint"
	case primitive.DataTypeCodeTinyint:
		return "tinyint"
	case primitive.DataTypeCodeDuration:
		return "duration"
	}
	return "?"
}

func (t *primitiveType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}
