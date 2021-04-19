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

var Blob PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBlob}

type BlobCodec struct{}

func (c *BlobCodec) Marshal(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, nil
	} else {
		var bytes []byte
		switch val := value.(type) {
		case string:
			bytes = []byte(val)
		case []byte:
			bytes = val
		default:
			return nil, fmt.Errorf("cannot marshal blob: incompatible value: %v", value)
		}
		return bytes, nil
	}
}

func (c *BlobCodec) Unmarshal(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	if len(encoded) == 0 {
		return "", nil
	} else {
		return string(encoded), nil
	}
}
