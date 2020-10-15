package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type AuthResponseCodec struct{}

func (c AuthResponseCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthResponse
}

func (c AuthResponseCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	authResponse := msg.(*message.AuthResponse)
	_, err := WriteBytes(authResponse.Token, dest)
	return err
}

func (c AuthResponseCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	authResponse := msg.(*message.AuthResponse)
	return SizeOfBytes(authResponse.Token), nil
}

func (c AuthResponseCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &message.AuthResponse{Token: token}, nil
}
