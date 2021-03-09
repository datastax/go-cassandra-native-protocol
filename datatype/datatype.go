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
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type DataType interface {
	GetDataTypeCode() primitive.DataTypeCode
	Clone() DataType
}

type encoder interface {
	encode(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error)
	encodedLength(t DataType, version primitive.ProtocolVersion) (length int, err error)
}

type decoder interface {
	decode(source io.Reader, version primitive.ProtocolVersion) (t DataType, err error)
}

type codec interface {
	encoder
	decoder
}

func WriteDataType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if err = primitive.WriteShort(uint16(t.GetDataTypeCode()), dest); err != nil {
		return fmt.Errorf("cannot write data type code %v: %w", t.GetDataTypeCode(), err)
	} else if codec, err := findCodec(t.GetDataTypeCode()); err != nil {
		return err
	} else if err = codec.encode(t, dest, version); err != nil {
		return fmt.Errorf("cannot write data type %v: %w", t, err)
	} else {
		return nil
	}
}

func LengthOfDataType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	length += primitive.LengthOfShort // type code
	if codec, err := findCodec(t.GetDataTypeCode()); err != nil {
		return -1, err
	} else if dataTypeLength, err := codec.encodedLength(t, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of data type %v: %w", t, err)
	} else {
		return length + dataTypeLength, nil
	}
}

func ReadDataType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	var typeCode uint16
	if typeCode, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read data type code: %w", err)
	} else if codec, err := findCodec(primitive.DataTypeCode(typeCode)); err != nil {
		return nil, err
	} else if decoded, err = codec.decode(source, version); err != nil {
		return nil, fmt.Errorf("cannot read data type code %v: %w", typeCode, err)
	} else {
		return decoded, nil
	}
}

var codecs = map[primitive.DataTypeCode]codec{
	primitive.DataTypeCodeAscii:     &primitiveTypeCodec{Ascii},
	primitive.DataTypeCodeBigint:    &primitiveTypeCodec{Bigint},
	primitive.DataTypeCodeBlob:      &primitiveTypeCodec{Blob},
	primitive.DataTypeCodeBoolean:   &primitiveTypeCodec{Boolean},
	primitive.DataTypeCodeCounter:   &primitiveTypeCodec{Counter},
	primitive.DataTypeCodeDecimal:   &primitiveTypeCodec{Decimal},
	primitive.DataTypeCodeDouble:    &primitiveTypeCodec{Double},
	primitive.DataTypeCodeFloat:     &primitiveTypeCodec{Float},
	primitive.DataTypeCodeInt:       &primitiveTypeCodec{Int},
	primitive.DataTypeCodeTimestamp: &primitiveTypeCodec{Timestamp},
	primitive.DataTypeCodeUuid:      &primitiveTypeCodec{Uuid},
	primitive.DataTypeCodeVarchar:   &primitiveTypeCodec{Varchar},
	primitive.DataTypeCodeVarint:    &primitiveTypeCodec{Varint},
	primitive.DataTypeCodeTimeuuid:  &primitiveTypeCodec{Timeuuid},
	primitive.DataTypeCodeInet:      &primitiveTypeCodec{Inet},
	primitive.DataTypeCodeDate:      &primitiveTypeCodec{Date},
	primitive.DataTypeCodeTime:      &primitiveTypeCodec{Time},
	primitive.DataTypeCodeSmallint:  &primitiveTypeCodec{Smallint},
	primitive.DataTypeCodeTinyint:   &primitiveTypeCodec{Tinyint},
	primitive.DataTypeCodeDuration:  &primitiveTypeCodec{Duration},
	primitive.DataTypeCodeList:      &listTypeCodec{},
	primitive.DataTypeCodeSet:       &setTypeCodec{},
	primitive.DataTypeCodeMap:       &mapTypeCodec{},
	primitive.DataTypeCodeTuple:     &tupleTypeCodec{},
	primitive.DataTypeCodeUdt:       &userDefinedTypeCodec{},
	primitive.DataTypeCodeCustom:    &customTypeCodec{},
}

func findCodec(code primitive.DataTypeCode) (codec, error) {
	codec, ok := codecs[code]
	if !ok {
		return nil, fmt.Errorf("cannot find codec for data type code %v", code)
	}
	return codec, nil
}

func CloneDataTypeSlice(o []DataType) []DataType {
	var newFieldTypes []DataType
	for _, fieldType := range o {
		if fieldType == nil {
			newFieldTypes = append(newFieldTypes, nil)
		} else {
			newFieldTypes = append(newFieldTypes, fieldType.Clone())
		}
	}

	return newFieldTypes
}
