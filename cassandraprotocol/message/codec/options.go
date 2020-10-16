package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type OptionsCodec struct{}

func (c OptionsCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	return nil
}

func (c OptionsCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c OptionsCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	return &message.Options{}, nil
}
