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

var Float PrimitiveType = &primitiveType{code: primitive.DataTypeCodeFloat}

const lengthOfFloat = 4

type FloatCodec struct{}

func (c *FloatCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, fmt.Errorf("expected float32, got nil")
	} else {
		var val float32
		switch v := value.(type) {
		case float32:
			val = v
		case string:
			i, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal float: invalid string: %v", value)
			}
			val = float32(i)
		default:
			return nil, fmt.Errorf("cannot marshal float: incompatible value: %v", value)
		}
		encoded = make([]byte, lengthOfFloat)
		binary.BigEndian.PutUint32(encoded, math.Float32bits(val))
		return
	}
}

func (c *FloatCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return float32(0), nil
	} else if length != lengthOfFloat {
		return float32(0), fmt.Errorf("cannot unmarshal float: expecting %v bytes but got: %v", lengthOfFloat, length)
	} else {
		value = math.Float32frombits(binary.BigEndian.Uint32(encoded))
		return
	}
}
