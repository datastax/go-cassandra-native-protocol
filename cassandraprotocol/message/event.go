package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Event struct {
	Type cassandraprotocol.EventType
}

func (m Event) IsResponse() bool {
	return true
}

func (m Event) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeEvent
}

// SCHEMA CHANGE EVENT

type SchemaChangeEvent struct {
	Event
	ChangeType cassandraprotocol.SchemaChangeType
	Target     cassandraprotocol.SchemaChangeTarget
	Keyspace   string
	Object     string
	Arguments  []string
}

func (m SchemaChangeEvent) String() string {
	return fmt.Sprintf("EVENT %v (change=%v target=%v keyspace=%v object=%v args=%v)",
		m.Type,
		m.ChangeType,
		m.Target,
		m.Keyspace,
		m.Object,
		m.Arguments)
}

// STATUS CHANGE EVENT

type StatusChangeEvent struct {
	Event
	ChangeType cassandraprotocol.StatusChangeType
	Address    *cassandraprotocol.Inet
}

func (m StatusChangeEvent) String() string {
	return fmt.Sprintf("EVENT %v (change=%v address=%v)", m.Type, m.ChangeType, m.Address)
}

// TOPOLOGY CHANGE EVENT

type TopologyChangeEvent struct {
	Event
	ChangeType cassandraprotocol.TopologyChangeType
	Address    *cassandraprotocol.Inet
}

func (m TopologyChangeEvent) String() string {
	return fmt.Sprintf("EVENT %v (change=%v address=%v)", m.Type, m.ChangeType, m.Address)
}
