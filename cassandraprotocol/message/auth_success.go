package message

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type AuthSuccess struct {
	Token []byte
}

func (m *AuthSuccess) IsResponse() bool {
	return true
}

func (m *AuthSuccess) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthSuccess
}

func (m *AuthSuccess) String() string {
	return "AUTH_SUCCESS token: " + base64.StdEncoding.EncodeToString(m.Token)
}

type AuthSuccessCodec struct{}

func (c *AuthSuccessCodec) Encode(msg Message, dest io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	authSuccess, ok := msg.(*AuthSuccess)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthSuccess, got %T", msg))
	}
	if authSuccess.Token == nil {
		return errors.New("AUTH_SUCCESS token cannot be nil")
	}
	return primitives.WriteBytes(authSuccess.Token, dest)
}

func (c *AuthSuccessCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authSuccess, ok := msg.(*AuthSuccess)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthSuccess, got %T", msg))
	}
	return primitives.LengthOfBytes(authSuccess.Token), nil
}

func (c *AuthSuccessCodec) Decode(source io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	if token, err := primitives.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthSuccess{Token: token}, nil
	}
}

func (c *AuthSuccessCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthSuccess
}
