package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"io"
)

type Supported struct {
	Options map[string][]string
}

func (m *Supported) IsResponse() bool {
	return true
}

func (m *Supported) GetOpCode() primitive.OpCode {
	return primitive.OpCodeSupported
}

func (m *Supported) String() string {
	return fmt.Sprintf("SUPPORTED %v", m.Options)
}

type SupportedCodec struct{}

func (c *SupportedCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	supported, ok := msg.(*Supported)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Supported, got %T", msg))
	}
	if err := primitive.WriteStringMultiMap(supported.Options, dest); err != nil {
		return err
	}
	return nil
}

func (c *SupportedCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	supported, ok := msg.(*Supported)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Supported, got %T", msg))
	}
	return primitive.LengthOfStringMultiMap(supported.Options), nil
}

func (c *SupportedCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if options, err := primitive.ReadStringMultiMap(source); err != nil {
		return nil, err
	} else {
		return &Supported{Options: options}, nil
	}
}

func (c *SupportedCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeSupported
}
