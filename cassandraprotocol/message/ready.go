package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"io"
)

type Ready struct {
}

func (m *Ready) IsResponse() bool {
	return true
}

func (m *Ready) GetOpCode() primitive.OpCode {
	return primitive.OpCodeReady
}

func (m *Ready) String() string {
	return "READY"
}

type ReadyCodec struct{}

func (c *ReadyCodec) Encode(msg Message, _ io.Writer, _ primitive.ProtocolVersion) error {
	_, ok := msg.(*Ready)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Ready, got %T", msg))
	}
	return nil
}

func (c *ReadyCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	_, ok := msg.(*Ready)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Ready, got %T", msg))
	}
	return 0, nil
}

func (c *ReadyCodec) Decode(_ io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	return &Ready{}, nil
}

func (c *ReadyCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeReady
}
