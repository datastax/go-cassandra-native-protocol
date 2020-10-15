package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Message interface {
	IsResponse() bool
	GetOpCode() cassandraprotocol.OpCode
}
