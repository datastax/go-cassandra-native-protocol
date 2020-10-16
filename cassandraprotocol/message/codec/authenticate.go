package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type AuthenticateCodec struct{}

func (c AuthenticateCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	authenticate := msg.(*message.Authenticate)
	_, err := primitive.WriteString(authenticate.Authenticator, dest)
	return err
}

func (c AuthenticateCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	authenticate := msg.(*message.Authenticate)
	return primitive.SizeOfString(authenticate.Authenticator), nil
}

func (c AuthenticateCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	authenticator, _, err := primitive.ReadString(source)
	if err != nil {
		return nil, err
	}
	return &message.Authenticate{Authenticator: authenticator}, nil
}
