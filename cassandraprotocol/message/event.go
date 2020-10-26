package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Event interface {
	Message
	GetEventType() cassandraprotocol.EventType
}

// SCHEMA CHANGE EVENT

// Note: this struct is identical to SchemaChangeResult
type SchemaChangeEvent struct {
	ChangeType cassandraprotocol.SchemaChangeType
	Target     cassandraprotocol.SchemaChangeTarget
	Keyspace   string
	Object     string
	Arguments  []string
}

func (m *SchemaChangeEvent) IsResponse() bool {
	return true
}

func (m *SchemaChangeEvent) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeEvent
}

func (m *SchemaChangeEvent) GetEventType() cassandraprotocol.EventType {
	return cassandraprotocol.EventTypeSchemaChange
}

func (m *SchemaChangeEvent) String() string {
	return fmt.Sprintf("EVENT SCHEMA CHANGE (type=%v target=%v keyspace=%v object=%v args=%v)",
		m.ChangeType,
		m.Target,
		m.Keyspace,
		m.Object,
		m.Arguments)
}

// STATUS CHANGE EVENT

type StatusChangeEvent struct {
	ChangeType cassandraprotocol.StatusChangeType
	Address    *primitives.Inet
}

func (m *StatusChangeEvent) IsResponse() bool {
	return true
}

func (m *StatusChangeEvent) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeEvent
}

func (m *StatusChangeEvent) GetEventType() cassandraprotocol.EventType {
	return cassandraprotocol.EventTypeStatusChange
}

func (m *StatusChangeEvent) String() string {
	return fmt.Sprintf("EVENT STATUS CHANGE (type=%v address=%v)", m.ChangeType, m.Address)
}

// TOPOLOGY CHANGE EVENT

type TopologyChangeEvent struct {
	ChangeType cassandraprotocol.TopologyChangeType
	Address    *primitives.Inet
}

func (m *TopologyChangeEvent) IsResponse() bool {
	return true
}

func (m *TopologyChangeEvent) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeEvent
}

func (m *TopologyChangeEvent) GetEventType() cassandraprotocol.EventType {
	return cassandraprotocol.EventTypeTopologyChange
}

func (m *TopologyChangeEvent) String() string {
	return fmt.Sprintf("EVENT TOPOLOGY CHANGE (type=%v address=%v)", m.ChangeType, m.Address)
}

// EVENT CODEC

type EventCodec struct{}

func (c *EventCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	event, ok := msg.(Event)
	if !ok {
		return fmt.Errorf("expected message.Event, got %T", msg)
	}
	if err = cassandraprotocol.CheckEventType(event.GetEventType()); err != nil {
		return err
	} else if err = primitives.WriteString(event.GetEventType(), dest); err != nil {
		return fmt.Errorf("cannot write EVENT type: %v", err)
	}
	switch event.GetEventType() {
	case cassandraprotocol.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.SchemaChangeEvent, got %T", msg)
		}
		if err = cassandraprotocol.CheckSchemaChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitives.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.ChangeType: %w", err)
		}
		if err = cassandraprotocol.CheckSchemaChangeTarget(sce.Target); err != nil {
			return err
		} else if err = primitives.WriteString(sce.Target, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.Target: %w", err)
		}
		if sce.Keyspace == "" {
			return errors.New("EVENT SchemaChange: cannot write empty keyspace")
		} else if err = primitives.WriteString(sce.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.Keyspace: %w", err)
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if sce.Object == "" {
				return errors.New("EVENT SchemaChange: cannot write empty object")
			} else if err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return fmt.Errorf("%s schema change targets are not supported in protocol version %d", sce.Target, version)
			}
			if sce.Keyspace == "" {
				return errors.New("EVENT SchemaChange: cannot write empty object")
			} else if err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
			}
			if err = primitives.WriteStringList(sce.Arguments, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Arguments: %w", err)
			}
		default:
			return fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
		return nil
	case cassandraprotocol.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.StatusChangeEvent, got %T", msg)
		}
		if err = cassandraprotocol.CheckStatusChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitives.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write StatusChangeEvent.ChangeType: %w", err)
		}
		if err = primitives.WriteInet(sce.Address, dest); err != nil {
			return fmt.Errorf("cannot write StatusChangeEvent.Address: %w", err)
		}
		return nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce, ok := msg.(*TopologyChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.TopologyChangeEvent, got %T", msg)
		}
		if err = cassandraprotocol.CheckTopologyChangeType(tce.ChangeType); err != nil {
			return err
		} else if err = primitives.WriteString(tce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.ChangeType: %w", err)
		}
		if err = primitives.WriteInet(tce.Address, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.Address: %w", err)
		}
		return nil
	}
	return errors.New("unknown EVENT type: " + event.GetEventType())
}

func (c *EventCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (length int, err error) {
	event, ok := msg.(Event)
	if !ok {
		return -1, fmt.Errorf("expected message.Event, got %T", msg)
	}
	length = primitives.LengthOfString(event.GetEventType())
	switch event.GetEventType() {
	case cassandraprotocol.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.SchemaChangeEvent, got %T", msg)
		}
		length += primitives.LengthOfString(sce.ChangeType)
		length += primitives.LengthOfString(sce.Target)
		length += primitives.LengthOfString(sce.Keyspace)
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			length += primitives.LengthOfString(sce.Object)
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			length += primitives.LengthOfString(sce.Object)
			length += primitives.LengthOfStringList(sce.Arguments)
		default:
			return -1, fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
		return length, nil
	case cassandraprotocol.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.StatusChangeEvent, got %T", msg)
		}
		length += primitives.LengthOfString(sce.ChangeType)
		inetLength, err := primitives.LengthOfInet(sce.Address)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of StatusChangeEvent.Address: %w", err)
		}
		length += inetLength
		return length, nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce, ok := msg.(*TopologyChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.TopologyChangeEvent, got %T", msg)
		}
		length += primitives.LengthOfString(tce.ChangeType)
		inetLength, err := primitives.LengthOfInet(tce.Address)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of TopologyChangeEvent.Address: %w", err)
		}
		length += inetLength
		return length, nil
	}
	return -1, errors.New("unknown EVENT type: " + event.GetEventType())
}

func (c *EventCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (Message, error) {
	eventType, err := primitives.ReadString(source)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case cassandraprotocol.EventTypeSchemaChange:
		sce := &SchemaChangeEvent{}
		if sce.ChangeType, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.ChangeType: %w", err)
		}
		if sce.Target, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.Target: %w", err)
		}
		if sce.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.Keyspace: %w", err)
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if sce.Object, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return nil, fmt.Errorf("%s schema change targets are not supported in protocol version %d", sce.Target, version)
			}
			if sce.Object, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
			if sce.Arguments, err = primitives.ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Arguments: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
		return sce, nil
	case cassandraprotocol.EventTypeStatusChange:
		sce := &StatusChangeEvent{}
		if sce.ChangeType, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.ChangeType: %w", err)
		}
		if sce.Address, err = primitives.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.Address: %w", err)
		}
		return sce, nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce := &TopologyChangeEvent{}
		if tce.ChangeType, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.ChangeType: %w", err)
		}
		if tce.Address, err = primitives.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.Address: %w", err)
		}
		return tce, nil
	}
	return nil, errors.New("unknown EVENT type: " + eventType)
}

func (c *EventCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeEvent
}
