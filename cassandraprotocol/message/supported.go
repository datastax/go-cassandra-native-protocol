package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Supported struct {
	Options map[string][]string
}

func (m *Supported) IsResponse() bool {
	return true
}

func (m *Supported) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeSupported
}

func (m *Supported) String() string {
	return fmt.Sprintf("SUPPORTED %v", m.Options)
}

type SupportedCodec struct{}

func (c *SupportedCodec) Encode(msg Message, dest io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	supported, ok := msg.(*Supported)
	if !ok {
		return errors.New(fmt.Sprintf("expected *Supported struct, got %T", msg))
	}
	if err := primitives.WriteStringMultiMap(supported.Options, dest); err != nil {
		return err
	}
	return nil
}

func (c *SupportedCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	supported, ok := msg.(*Supported)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *Supported struct, got %T", msg))
	}
	return primitives.LengthOfStringMultiMap(supported.Options), nil
}

func (c *SupportedCodec) Decode(source io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	if options, err := primitives.ReadStringMultiMap(source); err != nil {
		return nil, err
	} else {
		return &Supported{Options: options}, nil
	}
}

func (c *SupportedCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeSupported
}
