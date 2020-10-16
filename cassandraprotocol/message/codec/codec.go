package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type MessageEncoder interface {
	Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error
	EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error)
}

type MessageDecoder interface {
	Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error)
}

type MessageCodec interface {
	MessageEncoder
	MessageDecoder
}
