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
	"errors"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Ready is a response sent when the coordinator replies to a Startup request without requiring authentication.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Ready struct {
}

func (m *Ready) IsResponse() bool {
	return true
}

func (m *Ready) GetOpCode() primitive.OpCode {
	return primitive.OpCodeReady
}

func (m *Ready) String() string {
	return "READY"
}

type readyCodec struct{}

func (c *readyCodec) Encode(msg Message, _ io.Writer, _ primitive.ProtocolVersion) error {
	_, ok := msg.(*Ready)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Ready, got %T", msg))
	}
	return nil
}

func (c *readyCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	_, ok := msg.(*Ready)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Ready, got %T", msg))
	}
	return 0, nil
}

func (c *readyCodec) Decode(_ io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	return &Ready{}, nil
}

func (c *readyCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeReady
}
