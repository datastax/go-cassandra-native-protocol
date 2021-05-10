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
	"strconv"
)

var Boolean PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBoolean}

type BooleanCodec struct{}

func (c *BooleanCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val bool
		switch v := value.(type) {
		case int:
			val = v != 0
		case uint:
			val = v != 0
		case int64:
			val = v != 0
		case uint64:
			val = v != 0
		case int32:
			val = v != 0
		case uint32:
			val = v != 0
		case int16:
			val = v != 0
		case uint16:
			val = v != 0
		case int8:
			val = v != 0
		case uint8:
			val = v != 0
		case *big.Int:
			if v.IsInt64() {
				val = v.Int64() != 0
			} else {
				return nil, fmt.Errorf("cannot marshal boolean: value out of range: %v", value)
			}
		case string:
			val, err = strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal boolean: invalid string: %v", value)
			}
		default:
			return nil, fmt.Errorf("cannot marshal boolean: incompatible value: %v", value)
		}
		if val {
			return []byte{1}, nil
		} else {
			return []byte{0}, nil
		}
	}
}

func (c *BooleanCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if encoded == nil {
		return nil, nil
	} else if length != 1 {
		return false, fmt.Errorf("invalid boolean value, expecting 1 byte but got: %v", length)
	} else {
		value = encoded[0] != 0
		return
	}
}