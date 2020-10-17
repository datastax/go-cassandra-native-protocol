package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type AuthResponse struct {
	Token []byte
}

func (m AuthResponse) IsResponse() bool {
	return false
}

func (m AuthResponse) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthResponse
}

func (m AuthResponse) String() string {
	return "AUTH_RESPONSE " + string(m.Token)
}

type AuthResponseCodec struct{}

func (c AuthResponseCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authResponse := msg.(*AuthResponse)
	_, err := primitives.WriteBytes(authResponse.Token, dest)
	return err
}

func (c AuthResponseCodec) EncodedSize(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authResponse := msg.(*AuthResponse)
	return primitives.LengthOfBytes(authResponse.Token), nil
}

func (c AuthResponseCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	token, _, err := primitives.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthResponse{Token: token}, nil
}
