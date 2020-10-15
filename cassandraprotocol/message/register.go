package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Register struct {
	EventTypes []cassandraprotocol.EventType
}

func NewRegister(eventTypes []cassandraprotocol.EventType) *Register {
	return &Register{EventTypes: eventTypes}
}

func (m Register) IsResponse() bool {
	return false
}

func (m Register) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeRegister
}

func (m Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}
