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
)

var Varint PrimitiveType = &primitiveType{code: primitive.DataTypeCodeVarint}

type VarintCodec struct{}

func (c *VarintCodec) Marshal(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var val *big.Int
		switch v := value.(type) {
		case int:
			val = big.NewInt(int64(v))
		case uint:
			val = big.NewInt(int64(v))
		case int64:
			val = big.NewInt(v)
		case uint64:
			val = new(big.Int).SetUint64(v)
		case int32:
			val = big.NewInt(int64(v))
		case uint32:
			val = big.NewInt(int64(v))
		case int16:
			val = big.NewInt(int64(v))
		case uint16:
			val = big.NewInt(int64(v))
		case int8:
			val = big.NewInt(int64(v))
		case uint8:
			val = big.NewInt(int64(v))
		case float64:
			if v == float64(int64(v)) {
				val = big.NewInt(int64(v))
			} else {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
		case float32:
			if v == float32(int32(v)) {
				val = big.NewInt(int64(v))
			} else {
				return nil, fmt.Errorf("cannot marshal smallint: value out of range: %v", value)
			}
		case *big.Int:
			val = v
		case string:
			var ok bool
			if val, ok = new(big.Int).SetString(v, 10); !ok {
				return nil, fmt.Errorf("cannot marshal varint: invalid string: %v", value)
			}
		default:
			return nil, fmt.Errorf("cannot marshal varint: incompatible value: %v", value)
		}
		return val.Bytes(), nil
	}
}

func (c *VarintCodec) Unmarshal(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return nil, nil
	} else {
		value = (&big.Int{}).SetBytes(encoded)
		return
	}
}
