package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Authenticate struct {
	Authenticator string
}

func (m *Authenticate) IsResponse() bool {
	return true
}

func (m *Authenticate) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthenticate
}

func (m *Authenticate) String() string {
	return "AUTHENTICATE " + m.Authenticator
}

type AuthenticateCodec struct{}

func (c *AuthenticateCodec) Encode(msg Message, dest io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	authenticate := msg.(*Authenticate)
	return primitives.WriteString(authenticate.Authenticator, dest)
}

func (c *AuthenticateCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authenticate := msg.(*Authenticate)
	return primitives.LengthOfString(authenticate.Authenticator), nil
}

func (c *AuthenticateCodec) Decode(source io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	if authenticator, err := primitives.ReadString(source); err != nil {
		return nil, err
	} else {
		return &Authenticate{Authenticator: authenticator}, nil
	}
}

func (c *AuthenticateCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthenticate
}
