package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Startup struct {
	Options map[string]string
}

type StartupCustomizer func(*Startup)

func NewStartup(customizers ...StartupCustomizer) *Startup {
	startup := &Startup{map[string]string{"CQL_VERSION": "3.0.0"}}
	for _, customizer := range customizers {
		customizer(startup)
	}
	return startup
}

func WithCompression(compression string) StartupCustomizer {
	return func(startup *Startup) {
		startup.Options["COMPRESSION"] = compression
	}
}

func WithOptions(options map[string]string) StartupCustomizer {
	return func(startup *Startup) {
		for key, value := range options {
			startup.Options[key] = value
		}
	}
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
	return NewStartup(WithOptions(options)), nil
}

func (c *StartupCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeStartup
}
