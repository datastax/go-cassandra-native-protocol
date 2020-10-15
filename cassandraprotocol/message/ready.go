package message

import "go-cassandra-native-protocol/cassandraprotocol"

type Ready struct {
}

func (m Ready) IsResponse() bool {
	return false
}

func (m Ready) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeReady
}

func (m Ready) String() string {
	return "READY"
}
