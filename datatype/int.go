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
	"encoding/binary"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"math"
	"math/big"
	"strconv"
)

var Int PrimitiveType = &primitiveType{code: primitive.DataTypeCodeInt}

type IntCodec struct{}

func (c *IntCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val int32
		switch v := value.(type) {
		case int:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
			val = int32(v)
		case uint:
			if v > math.MaxInt32 {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
			val = int32(v)
		case int64:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
			val = int32(v)
		case uint64:
			if v > math.MaxInt32 {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
			val = int32(v)
		case int32:
			val = v
		case uint32:
			if v > math.MaxInt32 {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
			val = int32(v)
		case int16:
			val = int32(v)
		case uint16:
			val = int32(v)
		case int8:
			val = int32(v)
		case uint8:
			val = int32(v)
		case float64:
			if v == float64(int64(v)) {
				if v > math.MaxInt32 || v < math.MinInt32 {
					return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
				}
				val = int32(v)
			} else {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
		case float32:
			if v == float32(int32(v)) {
				val = int32(v)
			} else {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
		case *big.Int:
			if v.IsInt64() {
				i := v.Int64()
				if i > math.MaxInt32 || i < math.MinInt32 {
					return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
				}
				val = int32(i)
			} else {
				return nil, fmt.Errorf("cannot marshal int: value out of range: %v", value)
			}
		case string:
			i, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal int: invalid string: %v", value)
			}
			val = int32(i)
		default:
			return nil, fmt.Errorf("cannot marshal int: incompatible value: %v", value)
		}
		encoded = make([]byte, primitive.LengthOfInt)
		binary.BigEndian.PutUint32(encoded, uint32(val))
		return
	}
}

func (c *IntCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return int32(0), nil
	} else if length != primitive.LengthOfInt {
		return int32(0), fmt.Errorf("cannot unmarshal int: expecting %v bytes but got: %v", primitive.LengthOfInt, length)
	} else {
		value = int32(binary.BigEndian.Uint32(encoded))
		return
	}
}
