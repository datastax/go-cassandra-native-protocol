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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type AuthResponse struct {
	Token []byte
}

func (m *AuthResponse) IsResponse() bool {
	return false
}

func (m *AuthResponse) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthResponse
}

func (m *AuthResponse) String() string {
	return "AUTH_RESPONSE"
}

// Performs a deep copy of this message object.
func (m *AuthResponse) Clone() Message {
	return &AuthResponse{
		Token: primitive.CloneByteSlice(m.Token),
	}
}

type authResponseCodec struct{}

func (c *authResponseCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authResponse, ok := msg.(*AuthResponse)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthResponse, got %T", msg))
	}
	return primitive.WriteBytes(authResponse.Token, dest)
}

func (c *authResponseCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	authResponse, ok := msg.(*AuthResponse)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthResponse, got %T", msg))
	}
	return primitive.LengthOfBytes(authResponse.Token), nil
}

func (c *authResponseCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if token, err := primitive.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthResponse{Token: token}, nil
	}
}

func (c *authResponseCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthResponse
}
