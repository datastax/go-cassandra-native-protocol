package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type ReadyCodec struct{}

func (c ReadyCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeReady
}

func (c ReadyCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	return nil
}

func (c ReadyCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c ReadyCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	return &message.Ready{}, nil
}
