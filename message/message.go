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

package message

import (
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type Message interface {
	IsResponse() bool
	GetOpCode() primitive.OpCode
	DeepCopyMessage() Message
}

type Encoder interface {
	Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) error
	EncodedLength(msg Message, version primitive.ProtocolVersion) (int, error)
}

type Decoder interface {
	Decode(source io.Reader, version primitive.ProtocolVersion) (Message, error)
}

type Codec interface {
	Encoder
	Decoder
	GetOpCode() primitive.OpCode
}
