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
	"math/big"
	"net"
	"reflect"
)

type Codec interface {
	Encode(value interface{}, version primitive.ProtocolVersion) (encoded []byte, err error)
	Decode(encoded []byte, version primitive.ProtocolVersion) (value interface{}, err error)
}

var DefaultDecodeOutputTypes = map[DataType]reflect.Type{
	Ascii:    reflect.TypeOf((*string)(nil)).Elem(),
	Bigint:   reflect.TypeOf((*int64)(nil)).Elem(),
	Blob:     reflect.TypeOf((*string)(nil)).Elem(),
	Boolean:  reflect.TypeOf((*bool)(nil)).Elem(),
	Counter:  reflect.TypeOf((*int64)(nil)).Elem(),
	Decimal:  reflect.TypeOf((*Dec)(nil)),
	Double:   reflect.TypeOf((*float64)(nil)).Elem(),
	Float:    reflect.TypeOf((*float32)(nil)).Elem(),
	Inet:     reflect.TypeOf((*net.IP)(nil)).Elem(),
	Int:      reflect.TypeOf((*int32)(nil)).Elem(),
	Smallint: reflect.TypeOf((*int16)(nil)).Elem(),
	Text:     reflect.TypeOf((*string)(nil)).Elem(),
	Varchar:  reflect.TypeOf((*string)(nil)).Elem(),
	Timeuuid: reflect.TypeOf((*primitive.UUID)(nil)).Elem(),
	Tinyint:  reflect.TypeOf((*int8)(nil)).Elem(),
	Uuid:     reflect.TypeOf((*primitive.UUID)(nil)).Elem(),
	Varint:   reflect.TypeOf((*big.Int)(nil)),
}

var defaultOutputType = reflect.TypeOf([]byte{})

func getDatatypeDecodeOutputType(dataType DataType) reflect.Type {
	switch dataType.GetDataTypeCode() {
	case primitive.DataTypeCodeList:
		listType := dataType.(ListType)
		elemType := getDatatypeDecodeOutputType(listType.GetElementType())
		return reflect.SliceOf(elemType)
	case primitive.DataTypeCodeSet:
		setType := dataType.(SetType)
		elemType := getDatatypeDecodeOutputType(setType.GetElementType())
		return reflect.SliceOf(elemType)
	case primitive.DataTypeCodeMap:
		mapType := dataType.(MapType)
		keyType := getDatatypeDecodeOutputType(mapType.GetKeyType())
		valueType := getDatatypeDecodeOutputType(mapType.GetValueType())
		return reflect.MapOf(keyType, valueType)
	default:
		outputType, ok := DefaultDecodeOutputTypes[dataType]
		if !ok {
			return defaultOutputType
		}

		return outputType
	}
}

// NilDecoderCodec can be used to bypass the decoding of certain types. This codec will just pass through the encoded bytes
// when the Decode method is called. Encode is not supported.
type NilDecoderCodec struct {}

func (c *NilDecoderCodec) Encode(data interface{}, version primitive.ProtocolVersion) (encoded []byte, err error) {
	return nil, fmt.Errorf("NilDecoderCodec should only be used for decoding")
}

func (c *NilDecoderCodec) Decode(encoded []byte, version primitive.ProtocolVersion) (value interface{}, err error) {
	return encoded, nil
}