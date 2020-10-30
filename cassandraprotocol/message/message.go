package message

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Message interface {
	IsResponse() bool
	GetOpCode() primitives.OpCode
}

type Encoder interface {
	Encode(msg Message, dest io.Writer, version primitives.ProtocolVersion) error
	EncodedLength(msg Message, version primitives.ProtocolVersion) (int, error)
}

type Decoder interface {
	Decode(source io.Reader, version primitives.ProtocolVersion) (Message, error)
}

type Codec interface {
	Encoder
	Decoder
	GetOpCode() primitives.OpCode
}
