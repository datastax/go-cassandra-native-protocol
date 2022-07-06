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

// AuthSuccess is a response message sent in reply to an AuthResponse request, to indicate that the authentication was
// successful.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type AuthSuccess struct {
	Token []byte
}

func (m *AuthSuccess) IsResponse() bool {
	return true
}

func (m *AuthSuccess) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthSuccess
}

func (m *AuthSuccess) String() string {
	return "AUTH_SUCCESS"
}

type authSuccessCodec struct{}

func (c *authSuccessCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authSuccess, ok := msg.(*AuthSuccess)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthSuccess, got %T", msg))
	}
	// protocol specs allow the token to be null on AUTH SUCCESS
	return primitive.WriteBytes(authSuccess.Token, dest)
}

func (c *authSuccessCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	authSuccess, ok := msg.(*AuthSuccess)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthSuccess, got %T", msg))
	}
	return primitive.LengthOfBytes(authSuccess.Token), nil
}

func (c *authSuccessCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if token, err := primitive.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthSuccess{Token: token}, nil
	}
}

func (c *authSuccessCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthSuccess
}
