package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

type AuthResponse struct {
	Token []byte
}

func (m AuthResponse) IsResponse() bool {
	return false
}

func (m AuthResponse) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthResponse
}

func (m AuthResponse) String() string {
	return "AUTH_RESPONSE " + string(m.Token)
}
