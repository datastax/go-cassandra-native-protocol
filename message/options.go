package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Options struct {
}

func (m *Options) IsResponse() bool {
	return false
}

func (m *Options) GetOpCode() primitive.OpCode {
	return primitive.OpCodeOptions
}

func (m *Options) String() string {
	return "OPTIONS"
}

type optionsCodec struct{}

func (c *optionsCodec) Encode(msg Message, _ io.Writer, _ primitive.ProtocolVersion) error {
	_, ok := msg.(*Options)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Options, got %T", msg))
	}
	return nil
}

func (c *optionsCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	_, ok := msg.(*Options)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Options, got %T", msg))
	}
	return 0, nil
}

func (c *optionsCodec) Decode(_ io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	return &Options{}, nil
}

func (c *optionsCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeOptions
}
