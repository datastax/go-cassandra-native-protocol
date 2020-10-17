package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Authenticate struct {
	Authenticator string
}

func (m Authenticate) IsResponse() bool {
	return true
}

func (m Authenticate) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthenticate
}

func (m Authenticate) String() string {
	return "AUTHENTICATE " + m.Authenticator
}

type AuthenticateCodec struct{}

func (c AuthenticateCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authenticate := msg.(*Authenticate)
	_, err := primitives.WriteString(authenticate.Authenticator, dest)
	return err
}

func (c AuthenticateCodec) EncodedSize(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authenticate := msg.(*Authenticate)
	return primitives.LengthOfString(authenticate.Authenticator), nil
}

func (c AuthenticateCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	authenticator, _, err := primitives.ReadString(source)
	if err != nil {
		return nil, err
	}
	return &Authenticate{Authenticator: authenticator}, nil
}
