package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
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

func (m Startup) IsResponse() bool {
	return false
}

func (m Startup) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeStartup
}

func (m Startup) String() string {
	return fmt.Sprint("STARTUP ", m.Options)
}

// AUTHENTICATE
