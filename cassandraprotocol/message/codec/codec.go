package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type MessageEncoder interface {
	GetOpCode() cassandraprotocol.OpCode
	Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error
	EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error)
}

type MessageDecoder interface {
	GetOpCode() cassandraprotocol.OpCode
	Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error)
}
