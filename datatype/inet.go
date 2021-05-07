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
	"net"
)

type InetCodec struct{}

func (c *InetCodec) Encode(value interface{}, _ primitive.ProtocolVersion) (encoded []byte, err error) {
	if value == nil {
		return nil, fmt.Errorf("expected net.IP, got nil")
	} else {
		val, ok := value.(net.IP)
		if !ok {
			return nil, fmt.Errorf("cannot encode inet: incompatible value: %v", value)
		}

		t := val.To4()
		if t == nil {
			return val.To16(), nil
		}

		return t, nil
	}
}

func (c *InetCodec) Decode(encoded []byte, _ primitive.ProtocolVersion) (value interface{}, err error) {
	if encoded == nil {
		return nil, nil
	}

	var encodedLen int
	if encodedLen = len(encoded); !(encodedLen == 4 || encodedLen == 16) {
		return nil, fmt.Errorf(
			"cannot decode %v into %T: expected 4 or 16 bytes but got %d bytes", encoded, value, encodedLen)
	}
	b := make([]byte, encodedLen)
	copy(b, encoded)
	ip := net.IP(b)
	if v4 := ip.To4(); v4 != nil {
		return v4, nil
	}
	return ip, nil
}