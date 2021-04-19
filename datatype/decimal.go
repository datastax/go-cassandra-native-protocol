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
	"math/big"
)

type Dec struct {
	Unscaled *big.Int
	Scale    int32
}

var Decimal PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDecimal}

type DecimalCodec struct{}

func (c *DecimalCodec) Marshal(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else if val, ok := value.(*Dec); !ok {
		return nil, fmt.Errorf("cannot marshal decimal: incompatible value: %v", value)
	} else {
		unscaled := val.Unscaled.Bytes()
		encoded = make([]byte, primitive.LengthOfInt+len(unscaled))
		binary.BigEndian.PutUint32(encoded, uint32(val.Scale))
		encoded = append(encoded, unscaled...)
		return
	}
}

func (c *DecimalCodec) Unmarshal(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if length == 0 {
		return nil, nil
	} else if length <= primitive.LengthOfInt {
		return nil, fmt.Errorf("cannot unmarshal decimal: not enough bytes to read decimal: %v", length)
	} else {
		val := &Dec{}
		val.Scale = int32(binary.BigEndian.Uint32(encoded))
		val.Unscaled = (&big.Int{}).SetBytes(encoded[primitive.LengthOfInt:])
		return
	}
}
