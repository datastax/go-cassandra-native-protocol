package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type Ready struct {
}

func (m *Ready) IsResponse() bool {
	return false
}

func (m *Ready) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeReady
}

func (m *Ready) String() string {
	return "READY"
}

type ReadyCodec struct{}

func (c *ReadyCodec) Encode(msg Message, _ io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	_, ok := msg.(*Ready)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Ready, got %T", msg))
	}
	return nil
}

func (c *ReadyCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	_, ok := msg.(*Ready)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Ready, got %T", msg))
	}
	return 0, nil
}

func (c *ReadyCodec) Decode(_ io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	return &Ready{}, nil
}

func (c *ReadyCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeReady
}
