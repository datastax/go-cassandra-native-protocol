package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type AuthenticateCodec struct{}

func (c AuthenticateCodec) Encode(msg message.Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authenticate := msg.(*message.Authenticate)
	_, err := primitives.WriteString(authenticate.Authenticator, dest)
	return err
}

func (c AuthenticateCodec) EncodedSize(msg message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authenticate := msg.(*message.Authenticate)
	return primitives.LengthOfString(authenticate.Authenticator), nil
}

func (c AuthenticateCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	authenticator, _, err := primitives.ReadString(source)
	if err != nil {
		return nil, err
	}
	return &message.Authenticate{Authenticator: authenticator}, nil
}
