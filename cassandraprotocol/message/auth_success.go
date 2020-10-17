package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type AuthSuccess struct {
	Token []byte
}

func (m AuthSuccess) IsResponse() bool {
	return true
}

func (m AuthSuccess) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthSuccess
}

func (m AuthSuccess) String() string {
	return "AUTH_SUCCESS " + string(m.Token)
}

type AuthSuccessCodec struct{}

func (c AuthSuccessCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authSuccess := msg.(*AuthSuccess)
	_, err := primitives.WriteBytes(authSuccess.Token, dest)
	return err
}

func (c AuthSuccessCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authSuccess := msg.(*AuthSuccess)
	return primitives.LengthOfBytes(authSuccess.Token), nil
}

func (c AuthSuccessCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	token, _, err := primitives.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthSuccess{Token: token}, nil
}
