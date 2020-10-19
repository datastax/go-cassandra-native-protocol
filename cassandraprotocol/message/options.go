package message

import (
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

func (c *OptionsCodec) Encode(_ Message, _ io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	return nil
}

func (c *OptionsCodec) EncodedLength(_ Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c *OptionsCodec) Decode(_ io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	return &Options{}, nil
}

func (c *OptionsCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeOptions
}
