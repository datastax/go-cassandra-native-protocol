package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type AuthSuccessCodec struct{}

func (c AuthSuccessCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthSuccess
}

func (c AuthSuccessCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	authSuccess := msg.(*message.AuthSuccess)
	_, err := WriteBytes(authSuccess.Token, dest)
	return err
}

func (c AuthSuccessCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	authSuccess := msg.(*message.AuthSuccess)
	return SizeOfBytes(authSuccess.Token), nil
}

func (c AuthSuccessCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &message.AuthSuccess{Token: token}, nil
}
