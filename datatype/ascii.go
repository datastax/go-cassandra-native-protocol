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
	"unicode"
)

var Ascii PrimitiveType = &primitiveType{code: primitive.DataTypeCodeAscii}

type AsciiCodec struct{}

func (c *AsciiCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var bytes []byte
		switch val := value.(type) {
		case string:
			bytes = []byte(val)
		case []byte:
			bytes = val
		default:
			return nil, fmt.Errorf("cannot marshal ascii: incompatible value: %v", value)
		}
		if !isAscii(bytes) {
			return nil, fmt.Errorf("cannot marshal ascii: expected ASCII string, got: %v", value)
		} else {
			return bytes, nil
		}
	}
}

func (c *AsciiCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	if len(encoded) == 0 {
		return "", nil
	} else {
		return string(encoded), nil
	}
}

func isAscii(bytes []byte) bool {
	for i := 0; i < len(bytes); i++ {
		if bytes[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}
