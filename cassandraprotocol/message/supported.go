package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Supported struct {
	Options map[string][]string
}

func (m Supported) IsResponse() bool {
	return true
}

func (m Supported) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeSupported
}

func (m Supported) String() string {
	return fmt.Sprintf("SUPPORTED %v", m.Options)
}
