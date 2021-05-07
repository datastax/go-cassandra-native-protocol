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
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
	"math"
	"reflect"
)

type ListType interface {
	DataType
	GetElementType() DataType
}

type listType struct {
	elementType DataType
}

func (t *listType) GetElementType() DataType {
	return t.elementType
}

func NewListType(elementType DataType) ListType {
	return &listType{elementType: elementType}
}

func (t *listType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeList
}

func (t *listType) Clone() DataType {
	return &listType{
		elementType: t.elementType.Clone(),
	}
}

func (t *listType) String() string {
	return fmt.Sprintf("list<%v>", t.elementType)
}

func (t *listType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func writeListType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if listType, ok := t.(ListType); !ok {
		return fmt.Errorf("expected ListType, got %T", t)
	} else if err = WriteDataType(listType.GetElementType(), dest, version); err != nil {
		return fmt.Errorf("cannot write list element type: %w", err)
	}
	return nil
}

func lengthOfListType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	if listType, ok := t.(ListType); !ok {
		return -1, fmt.Errorf("expected ListType, got %T", t)
	} else if elementLength, err := LengthOfDataType(listType.GetElementType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of list element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func readListType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	listType := &listType{}
	if listType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read list element type: %w", err)
	}
	return listType, nil
}

type ListCodec struct {
	ElementCodec    Codec
}

func NewListCodec(elementCodec Codec) *ListCodec {
	return &ListCodec{ElementCodec: elementCodec}
}

func (c *ListCodec) Encode(data interface{}, version primitive.ProtocolVersion) (encoded []byte, err error) {
	if data == nil {
		return nil, nil
	}

	value := reflect.ValueOf(data)
	valueType := value.Type()
	valueKind := valueType.Kind()
	if valueKind == reflect.Slice && value.IsNil() {
		return nil, nil
	}

	if valueKind != reflect.Slice && valueKind != reflect.Array {
		return nil, fmt.Errorf("can not encode %T into list", data)
	}

	buf := &bytes.Buffer{}
	n := value.Len()

	if err := writeCollectionSize(version, n, buf); err != nil {
		return nil, err
	}

	for i := 0; i < n; i++ {
		item, err := c.ElementCodec.Encode(value.Index(i).Interface(), version)
		if err != nil {
			return nil, err
		}
		if err := writeCollectionSize(version, len(item), buf); err != nil {
			return nil, err
		}
		buf.Write(item)
	}
	return buf.Bytes(), nil
}

func (c *ListCodec) Decode(encoded []byte, version primitive.ProtocolVersion) (value interface{}, err error) {
	if encoded == nil {
		return nil, nil
	}

	n, read, err := readCollectionSize(version, encoded)
	if err != nil {
		return nil, err
	}

	if len(encoded) < n {
		return nil, fmt.Errorf("decode list: unexpected eof")
	}

	encoded = encoded[read:]
	slice := make([]interface{}, n)
	for i := 0; i < n; i++ {
		decodedElemValue, m, err := decodeChildElement(c.ElementCodec, encoded, version)
		if err != nil {
			return nil, err
		}
		encoded = encoded[m:]
		slice[i] = decodedElemValue
	}
	return slice, nil
}

func writeCollectionSize(version primitive.ProtocolVersion, n int, buf io.Writer) error {
	if version.Uses4BytesCollectionLength() {
		if n > math.MaxInt32 {
			return fmt.Errorf("could not encode collection: collection too large (%d elements)", n)
		}

		return primitive.WriteInt(int32(n), buf)
	} else {
		if n > math.MaxUint16 {
			return fmt.Errorf("could not encode collection: collection too large (%d elements)", n)
		}

		return primitive.WriteShort(uint16(n), buf)
	}
}

func readCollectionSize(version primitive.ProtocolVersion, encoded []byte) (int, int, error) {
	if version.Uses4BytesCollectionLength() {
		sizeInt32, err := primitive.ReadInt(bytes.NewBuffer(encoded))
		return int(sizeInt32), 4, err
	} else {
		sizeInt16, err := primitive.ReadShort(bytes.NewBuffer(encoded))
		return int(sizeInt16), 2, err
	}
}

func decodeChildElement(
	elementCodec Codec,
	encoded []byte,
	version primitive.ProtocolVersion) (output interface{}, read int, err error) {
	m, read, err := readCollectionSize(version, encoded)
	if err != nil {
		return nil, -1, err
	}
	encoded = encoded[read:]
	var decodedElemValue interface{}
	if m < 0 {
		decodedElemValue = nil
	} else if len(encoded) < m {
		return nil, -1, fmt.Errorf("decode list: unexpected eof")
	} else {
		decodedElem, err := elementCodec.Decode(encoded[:m], version)
		if err != nil {
			return nil, -1, err
		}
		decodedElemValue = decodedElem
	}

	return decodedElemValue, read + m, nil
}