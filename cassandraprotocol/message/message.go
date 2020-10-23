package message

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type Message interface {
	IsResponse() bool
	GetOpCode() cassandraprotocol.OpCode
}

type Encoder interface {
	Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) error
	EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (int, error)
}

type Decoder interface {
	Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (Message, error)
}

type Codec interface {
	Encoder
	Decoder
	GetOpCode() cassandraprotocol.OpCode
}
