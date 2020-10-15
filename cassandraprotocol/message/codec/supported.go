package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type SupportedCodec struct{}

func (c SupportedCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeSupported
}

func (c SupportedCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	supported := msg.(*message.Supported)
	_, err := WriteStringMultiMap(supported.Options, dest)
	if err != nil {
		return err
	}
	return nil
}

func (c SupportedCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	supported := msg.(*message.Supported)
	return SizeOfStringMultiMap(supported.Options), nil
}

func (c SupportedCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	options, _, err := ReadStringMultiMap(source)
	if err != nil {
		return nil, err
	}
	return &message.Supported{Options: options}, nil
}
