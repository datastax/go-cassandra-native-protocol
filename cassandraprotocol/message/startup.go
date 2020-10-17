package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Startup struct {
	Options map[string]string
}

func NewStartup() *Startup {
	return &Startup{map[string]string{"CQL_VERSION": "3.0.0"}}
}

func NewStartupWithCompression(compression string) *Startup {
	return &Startup{map[string]string{
		"CQL_VERSION": "3.0.0",
		"COMPRESSION": compression}}
}

func NewStartupWithOptions(options map[string]string) *Startup {
	return &Startup{options}
}

func (m *Startup) IsResponse() bool {
	return false
}

func (m *Startup) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeStartup
}

func (m *Startup) String() string {
	return fmt.Sprint("STARTUP ", m.Options)
}

type StartupCodec struct{}

func (c *StartupCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	startup := msg.(*Startup)
	_, err := primitives.WriteStringMap(startup.Options, dest)
	return err
}

func (c *StartupCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	startup := msg.(*Startup)
	return primitives.LengthOfStringMap(startup.Options), nil
}

func (c *StartupCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	options, _, err := primitives.ReadStringMap(source)
	if err != nil {
		return nil, err
	}
	return NewStartupWithOptions(options), nil
}

func (c *StartupCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeStartup
}
