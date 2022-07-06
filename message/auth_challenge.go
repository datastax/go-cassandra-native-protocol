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

// AuthChallenge is a response sent in reply to an AuthResponse request, when the server requires additional
// authentication data. It must be followed by an AuthResponse request message.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type AuthChallenge struct {
	Token []byte
}

func (m *AuthChallenge) IsResponse() bool {
	return true
}

func (m *AuthChallenge) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthChallenge
}

func (m *AuthChallenge) String() string {
	return "AUTH_CHALLENGE"
}

type authChallengeCodec struct{}

func (c *authChallengeCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	return primitive.WriteBytes(authChallenge.Token, dest)
}

func (c *authChallengeCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	return primitive.LengthOfBytes(authChallenge.Token), nil
}

func (c *authChallengeCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if token, err := primitive.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthChallenge{Token: token}, nil
	}
}

func (c *authChallengeCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthChallenge
}
