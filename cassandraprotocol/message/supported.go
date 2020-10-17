package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
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

func (c *SupportedCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	supported := msg.(*Supported)
	_, err := primitives.WriteStringMultiMap(supported.Options, dest)
	if err != nil {
		return err
	}
	return nil
}

func (c *SupportedCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	supported := msg.(*Supported)
	return primitives.LengthOfStringMultiMap(supported.Options), nil
}

func (c *SupportedCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	options, _, err := primitives.ReadStringMultiMap(source)
	if err != nil {
		return nil, err
	}
	return &Supported{Options: options}, nil
}
