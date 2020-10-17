package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type ReadyCodec struct{}

func (c ReadyCodec) Encode(_ message.Message, _ []byte, _ cassandraprotocol.ProtocolVersion) error {
	return nil
}

func (c ReadyCodec) EncodedSize(_ message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c ReadyCodec) Decode(_ []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	return &message.Ready{}, nil
}
