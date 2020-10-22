package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type Options struct {
}

func (m *Options) IsResponse() bool {
	return false
}

func (m *Options) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeOptions
}

func (m *Options) String() string {
	return "OPTIONS"
}

type OptionsCodec struct{}

func (c *OptionsCodec) Encode(msg Message, _ io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	_, ok := msg.(*Options)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Options, got %T", msg))
	}
	return nil
}

func (c *OptionsCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	_, ok := msg.(*Options)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Options, got %T", msg))
	}
	return 0, nil
}

func (c *OptionsCodec) Decode(_ io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	return &Options{}, nil
}

func (c *OptionsCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeOptions
}
