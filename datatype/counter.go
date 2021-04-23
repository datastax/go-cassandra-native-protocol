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
	"reflect"
	"strconv"
)

var Counter PrimitiveType = &primitiveType{code: primitive.DataTypeCodeCounter}

const lengthOfCounter = 8

type CounterCodec struct{}

func (c *CounterCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val int64
		switch v := value.(type) {
		case int:
			val = int64(v)
		case uint:
			if uint64(v) > math.MaxInt64 {
				return nil, fmt.Errorf("cannot marshal counter: value out of range: %v", value)
			}
			val = int64(v)
		case int64:
			val = v
		case uint64:
			if v > math.MaxInt64 {
				return nil, fmt.Errorf("cannot marshal counter: value out of range: %v", value)
			}
			val = int64(v)
		case int32:
			val = int64(v)
		case uint32:
			val = int64(v)
		case int16:
			val = int64(v)
		case uint16:
			val = int64(v)
		case int8:
			val = int64(v)
		case uint8:
			val = int64(v)
		case float64:
			if v == float64(int64(v)) {
				val = int64(v)
			} else {
				return nil, fmt.Errorf("cannot marshal counter: value out of range: %v", value)
			}
		case float32:
			if v == float32(int32(v)) {
				val = int64(v)
			} else {
				return nil, fmt.Errorf("cannot marshal counter: value out of range: %v", value)
			}
		case *big.Int:
			if v.IsInt64() {
				val = v.Int64()
			} else {
				return nil, fmt.Errorf("cannot marshal counter: value out of range: %v", value)
			}
		case string:
			val, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal counter: invalid string: %v", value)
			}
		default:
			return nil, fmt.Errorf("cannot marshal counter: incompatible value: %v", value)
		}
		encoded = make([]byte, lengthOfCounter)
		binary.BigEndian.PutUint64(encoded, uint64(val))
		return
	}
}

func (c *CounterCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return int64(0), nil
	} else if length != lengthOfCounter {
		return int64(0), fmt.Errorf("cannot unmarshal counter: expecting %v bytes but got: %v", lengthOfCounter, length)
	} else {
		value = int64(binary.BigEndian.Uint64(encoded))
		return
	}
}

func (c *CounterCodec) GetDecodeOutputType() reflect.Type {
	return getDatatypeDecodeOutputType(Counter)
}
