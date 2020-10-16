package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type SupportedCodec struct{}

func (c SupportedCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	supported := msg.(*message.Supported)
	_, err := primitive.WriteStringMultiMap(supported.Options, dest)
	if err != nil {
		return err
	}
	return nil
}

func (c SupportedCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	supported := msg.(*message.Supported)
	return primitive.SizeOfStringMultiMap(supported.Options), nil
}

func (c SupportedCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	options, _, err := primitive.ReadStringMultiMap(source)
	if err != nil {
		return nil, err
	}
	return &message.Supported{Options: options}, nil
}
