package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Message interface {
	IsResponse() bool
	GetOpCode() cassandraprotocol.OpCode
}

type Encoder interface {
	Encode(msg Message, dest []byte, version cassandraprotocol.ProtocolVersion) error
	EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (int, error)
}

type Decoder interface {
	Decode(source []byte, version cassandraprotocol.ProtocolVersion) (Message, error)
}

type Codec interface {
	Encoder
	Decoder
}
