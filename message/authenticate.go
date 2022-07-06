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

// Authenticate is a response sent in reply to a Startup request when the server requires authentication. It must be
// followed by an AuthResponse request message.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Authenticate struct {
	Authenticator string
}

func (m *Authenticate) IsResponse() bool {
	return true
}

func (m *Authenticate) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthenticate
}

func (m *Authenticate) String() string {
	return "AUTHENTICATE " + m.Authenticator
}

type authenticateCodec struct{}

func (c *authenticateCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authenticate, ok := msg.(*Authenticate)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Authenticate, got %T", msg))
	}
	if authenticate.Authenticator == "" {
		return errors.New("AUTHENTICATE authenticator cannot be empty")
	}
	return primitive.WriteString(authenticate.Authenticator, dest)
}

func (c *authenticateCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	authenticate, ok := msg.(*Authenticate)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Authenticate, got %T", msg))
	}
	return primitive.LengthOfString(authenticate.Authenticator), nil
}

func (c *authenticateCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if authenticator, err := primitive.ReadString(source); err != nil {
		return nil, err
	} else {
		return &Authenticate{Authenticator: authenticator}, nil
	}
}

func (c *authenticateCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthenticate
}
