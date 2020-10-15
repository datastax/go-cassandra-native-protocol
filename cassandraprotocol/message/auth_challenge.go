package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

type AuthChallenge struct {
	Token []byte
}

func (m AuthChallenge) IsResponse() bool {
	return true
}

func (m AuthChallenge) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthChallenge
}

func (m AuthChallenge) String() string {
	return "AUTH_CHALLENGE " + string(m.Token)
}
