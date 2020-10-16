package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Query struct {
	Query   string
	Options *QueryOptions
}

func (q Query) IsResponse() bool {
	return false
}

func (q Query) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeQuery
}
