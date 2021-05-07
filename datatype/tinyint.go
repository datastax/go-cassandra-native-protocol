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
	"math"
	"math/big"
	"strconv"
)

var Tinyint PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTinyint}

type TinyintCodec struct{}

func (c *TinyintCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val int8
		switch v := value.(type) {
		case int:
			if v > math.MaxInt8 || v < math.MinInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case uint:
			if v > math.MaxInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case int64:
			if v > math.MaxInt8 || v < math.MinInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case uint64:
			if v > math.MaxInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case int32:
			if v > math.MaxInt8 || v < math.MinInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case uint32:
			if v > math.MaxInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case int16:
			if v > math.MaxInt8 || v < math.MinInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case uint16:
			if v > math.MaxInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case int8:
			val = v
		case uint8:
			if v > math.MaxInt8 {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
			val = int8(v)
		case float64:
			if v == float64(int64(v)) {
				if v > math.MaxInt8 || v < math.MinInt8 {
					return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
				}
				val = int8(v)
			} else {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
		case float32:
			if v == float32(int32(v)) {
				if v > math.MaxInt8 || v < math.MinInt8 {
					return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
				}
				val = int8(v)
			} else {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
		case *big.Int:
			if v.IsInt64() {
				i := v.Int64()
				if i > math.MaxInt8 || i < math.MinInt8 {
					return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
				}
				val = int8(i)
			} else {
				return nil, fmt.Errorf("cannot marshal tinyint: value out of range: %v", value)
			}
		case string:
			i, err := strconv.ParseInt(v, 10, 8)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal tinyint: invalid string: %v", value)
			}
			val = int8(i)
		default:
			return nil, fmt.Errorf("cannot marshal tinyint: incompatible value: %v", value)
		}
		return []byte{uint8(val)}, nil
	}
}

func (c *TinyintCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if encoded == nil {
		return nil, nil
	} else if length > 1 {
		return int8(0), fmt.Errorf("cannot unmarshal tinyint: expecting 1 byte but got: %v", length)
	} else {
		return int8(encoded[0]), nil
	}
}