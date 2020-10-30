package message

import (
	"encoding/base64"
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
	return "AUTH_RESPONSE token: " + base64.StdEncoding.EncodeToString(m.Token)
}

type authResponseCodec struct{}

func (c *authResponseCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authResponse, ok := msg.(*AuthResponse)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthResponse, got %T", msg))
	}
	if authResponse.Token == nil {
		return errors.New("AUTH_RESPONSE token cannot be nil")
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
