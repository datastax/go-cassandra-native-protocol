package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Authenticate struct {
	Authenticator string
}

func (m Authenticate) IsResponse() bool {
	return true
}

func (m Authenticate) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthenticate
}

func (m Authenticate) String() string {
	return "AUTHENTICATE " + m.Authenticator
}
