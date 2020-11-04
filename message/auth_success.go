package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

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
	if authSuccess.Token == nil {
		return errors.New("AUTH_SUCCESS token cannot be nil")
	}
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
