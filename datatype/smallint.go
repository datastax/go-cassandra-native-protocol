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

var Smallint PrimitiveType = &primitiveType{code: primitive.DataTypeCodeSmallint}

type SmallintCodec struct{}

func (c *SmallintCodec) Marshal(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val int16
		switch v := value.(type) {
		case int:
			if v > math.MaxInt16 || v < math.MinInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case uint:
			if v > math.MaxInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case int64:
			if v > math.MaxInt16 || v < math.MinInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case uint64:
			if v > math.MaxInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case int32:
			if v > math.MaxInt16 || v < math.MinInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case uint32:
			if v > math.MaxInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case int16:
			val = v
		case uint16:
			if v > math.MaxInt16 {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
			val = int16(v)
		case int8:
			val = int16(v)
		case uint8:
			val = int16(v)
		case float64:
			if v == float64(int64(v)) {
				if v > math.MaxInt16 || v < math.MinInt16 {
					return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
				}
				val = int16(v)
			} else {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
		case float32:
			if v == float32(int32(v)) {
				if v > math.MaxInt16 || v < math.MinInt16 {
					return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
				}
				val = int16(v)
			} else {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
		case *big.Int:
			if v.IsInt64() {
				i := v.Int64()
				if i > math.MaxInt16 || i < math.MinInt16 {
					return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
				}
				val = int16(i)
			} else {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
		case string:
			i, err := strconv.ParseInt(v, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal smallint: invalid string: %v", value)
			}
			val = int16(i)
		default:
			return nil, fmt.Errorf("cannot marshal smallint: incompatible value: %v", value)
		}
		encoded = make([]byte, primitive.LengthOfShort)
		binary.BigEndian.PutUint16(encoded, uint16(val))
		return
	}
}

func (c *SmallintCodec) Unmarshal(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return int16(0), nil
	} else if length != primitive.LengthOfShort {
		return int16(0), fmt.Errorf("cannot unmarshal smallint: expecting %v bytes but got: %v", primitive.LengthOfShort, length)
	} else {
		value = int16(binary.BigEndian.Uint16(encoded))
		return
	}
}
