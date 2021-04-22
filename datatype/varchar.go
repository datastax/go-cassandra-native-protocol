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

var Varchar PrimitiveType = &primitiveType{code: primitive.DataTypeCodeVarchar}

type VarcharCodec struct{}

func (c *VarcharCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		switch val := value.(type) {
		case string:
			return []byte(val), nil
		case []byte:
			return val, nil
		default:
			return nil, fmt.Errorf("cannot marshal varchar: incompatible value: %v", value)
		}
	}
}

func (c *VarcharCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	if len(encoded) == 0 {
		return "", nil
	} else {
		return string(encoded), nil
	}
}
