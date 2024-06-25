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
	Ascii     = &PrimitiveType{code: primitive.DataTypeCodeAscii}
	Bigint    = &PrimitiveType{code: primitive.DataTypeCodeBigint}
	Blob      = &PrimitiveType{code: primitive.DataTypeCodeBlob}
	Boolean   = &PrimitiveType{code: primitive.DataTypeCodeBoolean}
	Counter   = &PrimitiveType{code: primitive.DataTypeCodeCounter}
	Date      = &PrimitiveType{code: primitive.DataTypeCodeDate}
	Decimal   = &PrimitiveType{code: primitive.DataTypeCodeDecimal}
	Double    = &PrimitiveType{code: primitive.DataTypeCodeDouble}
	Duration  = &PrimitiveType{code: primitive.DataTypeCodeDuration}
	Float     = &PrimitiveType{code: primitive.DataTypeCodeFloat}
	Inet      = &PrimitiveType{code: primitive.DataTypeCodeInet}
	Int       = &PrimitiveType{code: primitive.DataTypeCodeInt}
	Smallint  = &PrimitiveType{code: primitive.DataTypeCodeSmallint}
	Time      = &PrimitiveType{code: primitive.DataTypeCodeTime}
	Timestamp = &PrimitiveType{code: primitive.DataTypeCodeTimestamp}
	Timeuuid  = &PrimitiveType{code: primitive.DataTypeCodeTimeuuid}
	Tinyint   = &PrimitiveType{code: primitive.DataTypeCodeTinyint}
	Uuid      = &PrimitiveType{code: primitive.DataTypeCodeUuid}
	Varchar   = &PrimitiveType{code: primitive.DataTypeCodeVarchar}
	Varint    = &PrimitiveType{code: primitive.DataTypeCodeVarint}
)

// PrimitiveType is a datatype that is represented by a CQL scalar type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type PrimitiveType struct {
	code primitive.DataTypeCode
}

func (t *PrimitiveType) Code() primitive.DataTypeCode {
	return t.code
}

func (t *PrimitiveType) String() string {
	return t.AsCql()
}

func (t *PrimitiveType) AsCql() string {
	switch t.Code() {
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
		return "time"
	case primitive.DataTypeCodeSmallint:
		return "smallint"
	case primitive.DataTypeCodeTinyint:
		return "tinyint"
	case primitive.DataTypeCodeDuration:
		return "duration"
	}
	return "?"
}
