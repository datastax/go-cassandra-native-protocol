package message

import "go-cassandra-native-protocol/cassandraprotocol"

type AuthSuccess struct {
	Token []byte
}

func (m AuthSuccess) IsResponse() bool {
	return true
}

func (m AuthSuccess) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthSuccess
}

func (m AuthSuccess) String() string {
	return "AUTH_SUCCESS " + string(m.Token)
}
