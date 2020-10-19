package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Event interface {
	Message
	GetEventType() cassandraprotocol.EventType
}

// SCHEMA CHANGE EVENT

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
	Address    *cassandraprotocol.Inet
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
	Address    *cassandraprotocol.Inet
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

func (c *EventCodec) Encode(msg Message, dest []byte, version cassandraprotocol.ProtocolVersion) (err error) {
	event, ok := msg.(Event)
	if !ok {
		return errors.New(fmt.Sprintf("expected Event interface, got %T", msg))
	}
	if dest, err = primitives.WriteString(event.GetEventType(), dest); err != nil {
		return err
	}
	switch event.GetEventType() {
	case cassandraprotocol.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected SchemaChangeEvent struct, got %T", msg))
		}
		switch sce.ChangeType {
		case cassandraprotocol.SchemaChangeTypeCreated:
		case cassandraprotocol.SchemaChangeTypeUpdated:
		case cassandraprotocol.SchemaChangeTypeDropped:
		default:
			return errors.New(fmt.Sprintf("unknown schema change type: %v", sce.Target))
		}
		if dest, err = primitives.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.ChangeType: %w", err)
		}
		if dest, err = primitives.WriteString(sce.Target, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.Target: %w", err)
		}
		if dest, err = primitives.WriteString(sce.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.Keyspace: %w", err)
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if dest, err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			if dest, err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
			}
			if dest, err = primitives.WriteStringList(sce.Arguments, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Arguments: %w", err)
			}
		default:
			return errors.New(fmt.Sprintf("unknown schema change target: %v", sce.Target))
		}
		return nil
	case cassandraprotocol.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected StatusChangeEvent struct, got %T", msg))
		}
		switch sce.ChangeType {
		case cassandraprotocol.StatusChangeTypeUp:
		case cassandraprotocol.StatusChangeTypeDown:
		default:
			return errors.New(fmt.Sprintf("unknown status change type: %v", sce.ChangeType))
		}
		if dest, err = primitives.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write StatusChangeEvent.ChangeType: %w", err)
		}
		if dest, err = primitives.WriteInet(sce.Address, dest); err != nil {
			return fmt.Errorf("cannot write StatusChangeEvent.Address: %w", err)
		}
		return nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce, ok := msg.(*TopologyChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected TopologyChangeEvent struct, got %T", msg))
		}
		switch tce.ChangeType {
		case cassandraprotocol.TopologyChangeTypeNewNode:
		case cassandraprotocol.TopologyChangeTypeRemovedNode:
		default:
			return errors.New(fmt.Sprintf("unknown topology change type: %v", tce.ChangeType))
		}
		if dest, err = primitives.WriteString(tce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.ChangeType: %w", err)
		}
		if dest, err = primitives.WriteInet(tce.Address, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.Address: %w", err)
		}
		return nil
	}
	return errors.New("unknown EVENT type: " + event.GetEventType())
}

func (c *EventCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	event, ok := msg.(Event)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected Event interface, got %T", msg))
	}
	length = primitives.LengthOfString(event.GetEventType())
	switch event.GetEventType() {
	case cassandraprotocol.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected SchemaChangeEvent struct, got %T", msg))
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
			if version < cassandraprotocol.ProtocolVersion4 {
				return -1, errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			length += primitives.LengthOfString(sce.Object)
			length += primitives.LengthOfStringList(sce.Arguments)
		default:
			return -1, errors.New(fmt.Sprintf("unknown schema change target: %v", sce.Target))
		}
		return length, nil
	case cassandraprotocol.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected StatusChangeEvent struct, got %T", msg))
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
			return -1, errors.New(fmt.Sprintf("expected TopologyChangeEvent struct, got %T", msg))
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

func (c *EventCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (Message, error) {
	eventType, _, err := primitives.ReadString(source)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case cassandraprotocol.EventTypeSchemaChange:
		sce := &SchemaChangeEvent{}
		if sce.ChangeType, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.ChangeType: %w", err)
		}
		if sce.Target, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.Target: %w", err)
		}
		if sce.Keyspace, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.Keyspace: %w", err)
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if sce.Object, source, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return nil, errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			if sce.Object, source, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
			if sce.Arguments, source, err = primitives.ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Arguments: %w", err)
			}
		default:
			return nil, errors.New(fmt.Sprintf("unknown schema change target: %v", sce.Target))
		}
		return sce, nil
	case cassandraprotocol.EventTypeStatusChange:
		sce := &StatusChangeEvent{}
		if sce.ChangeType, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.ChangeType: %w", err)
		}
		if sce.Address, source, err = primitives.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.Address: %w", err)
		}
		return sce, nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce := &TopologyChangeEvent{}
		if tce.ChangeType, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.ChangeType: %w", err)
		}
		if tce.Address, source, err = primitives.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.Address: %w", err)
		}
		return tce, nil
	}
	return nil, errors.New("unknown EVENT type: " + eventType)
}

func (c *EventCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeEvent
}
