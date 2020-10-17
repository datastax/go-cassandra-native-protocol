package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type AuthSuccessCodec struct{}

func (c AuthSuccessCodec) Encode(msg message.Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authSuccess := msg.(*message.AuthSuccess)
	_, err := primitives.WriteBytes(authSuccess.Token, dest)
	return err
}

func (c AuthSuccessCodec) EncodedSize(msg message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authSuccess := msg.(*message.AuthSuccess)
	return primitives.LengthOfBytes(authSuccess.Token), nil
}

func (c AuthSuccessCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	token, _, err := primitives.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &message.AuthSuccess{Token: token}, nil
}
