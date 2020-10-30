package message

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type AuthResponse struct {
	Token []byte
}

func (m *AuthResponse) IsResponse() bool {
	return false
}

func (m *AuthResponse) GetOpCode() primitives.OpCode {
	return primitives.OpCodeAuthResponse
}

func (m *AuthResponse) String() string {
	return "AUTH_RESPONSE token: " + base64.StdEncoding.EncodeToString(m.Token)
}

type AuthResponseCodec struct{}

func (c *AuthResponseCodec) Encode(msg Message, dest io.Writer, _ primitives.ProtocolVersion) error {
	authResponse, ok := msg.(*AuthResponse)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthResponse, got %T", msg))
	}
	if authResponse.Token == nil {
		return errors.New("AUTH_RESPONSE token cannot be nil")
	}
	return primitives.WriteBytes(authResponse.Token, dest)
}

func (c *AuthResponseCodec) EncodedLength(msg Message, _ primitives.ProtocolVersion) (int, error) {
	authResponse, ok := msg.(*AuthResponse)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthResponse, got %T", msg))
	}
	return primitives.LengthOfBytes(authResponse.Token), nil
}

func (c *AuthResponseCodec) Decode(source io.Reader, _ primitives.ProtocolVersion) (Message, error) {
	if token, err := primitives.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthResponse{Token: token}, nil
	}
}

func (c *AuthResponseCodec) GetOpCode() primitives.OpCode {
	return primitives.OpCodeAuthResponse
}
