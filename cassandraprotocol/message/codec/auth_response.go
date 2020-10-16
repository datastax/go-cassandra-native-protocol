package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type AuthResponseCodec struct{}

func (c AuthResponseCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	authResponse := msg.(*message.AuthResponse)
	_, err := primitive.WriteBytes(authResponse.Token, dest)
	return err
}

func (c AuthResponseCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	authResponse := msg.(*message.AuthResponse)
	return primitive.SizeOfBytes(authResponse.Token), nil
}

func (c AuthResponseCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	token, _, err := primitive.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &message.AuthResponse{Token: token}, nil
}
