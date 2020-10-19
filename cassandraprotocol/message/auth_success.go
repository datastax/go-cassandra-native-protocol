package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
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
	return "AUTH_SUCCESS " + string(m.Token)
}

type AuthSuccessCodec struct{}

func (c *AuthSuccessCodec) Encode(msg Message, dest io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	authSuccess := msg.(*AuthSuccess)
	return primitives.WriteBytes(authSuccess.Token, dest)
}

func (c *AuthSuccessCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authSuccess := msg.(*AuthSuccess)
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
