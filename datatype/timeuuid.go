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
)

var Timeuuid PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTimeuuid}

type TimeuuidCodec struct{}

func (c *TimeuuidCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		switch v := value.(type) {
		case primitive.UUID:
			encoded = v.Bytes()
		case *primitive.UUID:
			encoded = v.Bytes()
		case []byte:
			if len(v) != 16 {
				return nil, fmt.Errorf("cannot marshal Timeuuidtimeuuided 16 bytes, got: %v", len(v))
			}
			encoded = v
		case string:
			u, err := primitive.ParseUuid(v)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal timeuuid: %w", err)
			}
			encoded = u.Bytes()
		default:
			return nil, fmt.Errorf("cannot marshal timeuuid: incompatible value: %v", value)
		}
		return
	}
}

func (c *TimeuuidCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	length := len(encoded)
	if encoded == nil {
		return nil, nil
	} else if length != primitive.LengthOfUuid {
		return nil, fmt.Errorf("cannot unmarshal timeuuid: expected 16 bytes, got: %v", length)
	} else {
		val := &primitive.UUID{}
		copy(val[:], encoded)
		return *val, nil
	}
}