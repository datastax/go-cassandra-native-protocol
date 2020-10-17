package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type OptionsCodec struct{}

func (c OptionsCodec) Encode(_ message.Message, _ []byte, _ cassandraprotocol.ProtocolVersion) error {
	return nil
}

func (c OptionsCodec) EncodedSize(_ message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c OptionsCodec) Decode(_ []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	return &message.Options{}, nil
}
