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
	"strconv"
)

var Double PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDouble}

const lengthOfDouble = 8

type DoubleCodec struct{}

func (c *DoubleCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val float64
		switch v := value.(type) {
		case float64:
			val = v
		case string:
			val, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal double: invalid string: %v", value)
			}
		default:
			return nil, fmt.Errorf("cannot marshal double: incompatible value: %v", value)
		}
		encoded = make([]byte, lengthOfDouble)
		binary.BigEndian.PutUint64(encoded, math.Float64bits(val))
		return
	}
}

func (c *DoubleCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return float64(0), nil
	} else if length != lengthOfDouble {
		return float64(0), fmt.Errorf("cannot unmarshal double: expecting %v bytes but got: %v", lengthOfDouble, length)
	} else {
		value = math.Float64frombits(binary.BigEndian.Uint64(encoded))
		return
	}
}
