package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type AuthenticateCodec struct{}

func (c AuthenticateCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthenticate
}

func (c AuthenticateCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	authenticate := msg.(*message.Authenticate)
	_, err := WriteString(authenticate.Authenticator, dest)
	return err
}

func (c AuthenticateCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	authenticate := msg.(*message.Authenticate)
	return SizeOfString(authenticate.Authenticator), nil
}

func (c AuthenticateCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	authenticator, _, err := ReadString(source)
	if err != nil {
		return nil, err
	}
	return &message.Authenticate{Authenticator: authenticator}, nil
}
