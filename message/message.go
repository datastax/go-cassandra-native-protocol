package message

import (
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Message interface {
	IsResponse() bool
	GetOpCode() primitive.OpCode
}

type Encoder interface {
	Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) error
	EncodedLength(msg Message, version primitive.ProtocolVersion) (int, error)
}

type Decoder interface {
	Decode(source io.Reader, version primitive.ProtocolVersion) (Message, error)
}

type Codec interface {
	Encoder
	Decoder
	GetOpCode() primitive.OpCode
}
