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

package datacodec

import (
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Blob is a codec for the CQL blob type. Its preferred Go type is []byte, but it can encode from and decode to
// string as well. When given a []byte source or destination, the encoding and decoding operations are actually no-ops,
// that is: the []byte value is passed along as is.
var Blob Codec = &blobCodec{dataType: datatype.Blob}

// PassThrough is another name for the Blob codec.
var PassThrough = Blob

// NewCustom returns a codec for the CQL custom type. Its preferred Go type is []byte, but it can encode from and decode
// to string as well. This codec is identical to the Blob codec.
func NewCustom(customType *datatype.Custom) Codec {
	return &blobCodec{dataType: customType}
}

type blobCodec struct {
	dataType datatype.DataType
}

func (c *blobCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *blobCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if dest, err = convertToBytes(source); err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *blobCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if wasNull, err = convertFromBytes(source, dest); err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToBytes(source interface{}) (val []byte, err error) {
	switch s := source.(type) {
	case string:
		val = []byte(s)
	case []byte:
		val = s
	case *string:
		if s != nil {
			val = []byte(*s)
		}
	case *[]byte:
		if s != nil {
			val = *s
		}
	case nil:
	default:
		err = ErrConversionNotSupported
	}
	if err != nil {
		err = errSourceConversionFailed(source, val, err)
	}
	return
}

func convertFromBytes(val []byte, dest interface{}) (wasNull bool, err error) {
	wasNull = val == nil
	switch d := dest.(type) {
	case *interface{}:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = val
		}
	case *string:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = string(val)
		}
	case *[]byte:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = val
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestinationConversionFailed(val, dest, err)
	}
	return
}
