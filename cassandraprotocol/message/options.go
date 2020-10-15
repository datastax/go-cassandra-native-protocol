package message

import "go-cassandra-native-protocol/cassandraprotocol"

type Options struct {
}

func (m Options) IsResponse() bool {
	return false
}

func (m Options) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeOptions
}

func (m Options) String() string {
	return "OPTIONS"
}
