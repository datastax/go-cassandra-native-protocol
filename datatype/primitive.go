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
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

var (
	Ascii     PrimitiveType = &primitiveType{code: primitive.DataTypeCodeAscii}
	Bigint    PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBigint}
	Blob      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBlob}
	Boolean   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBoolean}
	Counter   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeCounter}
	Decimal   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDecimal}
	Double    PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDouble}
	Float     PrimitiveType = &primitiveType{code: primitive.DataTypeCodeFloat}
	Int       PrimitiveType = &primitiveType{code: primitive.DataTypeCodeInt}
	Timestamp PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTimestamp}
	Uuid      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeUuid}
	Varchar   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeVarchar}
	Varint    PrimitiveType = &primitiveType{code: primitive.DataTypeCodeVarint}
	Timeuuid  PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTimeuuid}
	Inet      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeInet}
	Date      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDate}
	Time      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTime}
	Smallint  PrimitiveType = &primitiveType{code: primitive.DataTypeCodeSmallint}
	Tinyint   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTinyint}
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

type primitiveTypeCodec struct {
	primitiveType PrimitiveType
}

func (c *primitiveTypeCodec) encode(t DataType, _ io.Writer, version primitive.ProtocolVersion) (err error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := primitive.CheckValidDataTypeCode(t.GetDataTypeCode()); err != nil {
		return err
	}
	if version < primitive.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == primitive.DataTypeCodeDuration {
		return fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return nil
}

func (c *primitiveTypeCodec) encodedLength(t DataType, _ primitive.ProtocolVersion) (int, error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := primitive.CheckValidDataTypeCode(t.GetDataTypeCode()); err != nil {
		return -1, err
	}
	return 0, nil
}

func (c *primitiveTypeCodec) decode(_ io.Reader, version primitive.ProtocolVersion) (t DataType, err error) {
	if version < primitive.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == primitive.DataTypeCodeDuration {
		return nil, fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return c.primitiveType, nil
}
