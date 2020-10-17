package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type SupportedCodec struct{}

func (c SupportedCodec) Encode(msg message.Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	supported := msg.(*message.Supported)
	_, err := primitives.WriteStringMultiMap(supported.Options, dest)
	if err != nil {
		return err
	}
	return nil
}

func (c SupportedCodec) EncodedSize(msg message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	supported := msg.(*message.Supported)
	return primitives.LengthOfStringMultiMap(supported.Options), nil
}

func (c SupportedCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	options, _, err := primitives.ReadStringMultiMap(source)
	if err != nil {
		return nil, err
	}
	return &message.Supported{Options: options}, nil
}
