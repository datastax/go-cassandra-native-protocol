package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type StartupCodec struct{}

func (c StartupCodec) Encode(msg message.Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	startup := msg.(*message.Startup)
	_, err := primitives.WriteStringMap(startup.Options, dest)
	return err
}

func (c StartupCodec) EncodedSize(msg message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	startup := msg.(*message.Startup)
	return primitives.LengthOfStringMap(startup.Options), nil
}

func (c StartupCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	options, _, err := primitives.ReadStringMap(source)
	if err != nil {
		return nil, err
	}
	return message.NewStartupWithOptions(options), nil
}
