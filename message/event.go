package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Event interface {
	Message
	GetEventType() primitive.EventType
}

// SCHEMA CHANGE EVENT

// Note: this struct is identical to SchemaChangeResult
type SchemaChangeEvent struct {
	ChangeType primitive.SchemaChangeType
	Target     primitive.SchemaChangeTarget
	Keyspace   string
	Object     string
	Arguments  []string
}

func (m *SchemaChangeEvent) IsResponse() bool {
	return true
}

func (m *SchemaChangeEvent) GetOpCode() primitive.OpCode {
	return primitive.OpCodeEvent
}

func (m *SchemaChangeEvent) GetEventType() primitive.EventType {
	return primitive.EventTypeSchemaChange
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
	ChangeType primitive.StatusChangeType
	Address    *primitive.Inet
}

func (m *StatusChangeEvent) IsResponse() bool {
	return true
}

func (m *StatusChangeEvent) GetOpCode() primitive.OpCode {
	return primitive.OpCodeEvent
}

func (m *StatusChangeEvent) GetEventType() primitive.EventType {
	return primitive.EventTypeStatusChange
}

func (m *StatusChangeEvent) String() string {
	return fmt.Sprintf("EVENT STATUS CHANGE (type=%v address=%v)", m.ChangeType, m.Address)
}

// TOPOLOGY CHANGE EVENT

type TopologyChangeEvent struct {
	ChangeType primitive.TopologyChangeType
	Address    *primitive.Inet
}

func (m *TopologyChangeEvent) IsResponse() bool {
	return true
}

func (m *TopologyChangeEvent) GetOpCode() primitive.OpCode {
	return primitive.OpCodeEvent
}

func (m *TopologyChangeEvent) GetEventType() primitive.EventType {
	return primitive.EventTypeTopologyChange
}

func (m *TopologyChangeEvent) String() string {
	return fmt.Sprintf("EVENT TOPOLOGY CHANGE (type=%v address=%v)", m.ChangeType, m.Address)
}

// EVENT CODEC

type EventCodec struct{}

func (c *EventCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	event, ok := msg.(Event)
	if !ok {
		return fmt.Errorf("expected message.Event, got %T", msg)
	}
	if err = primitive.CheckEventType(event.GetEventType()); err != nil {
		return err
	} else if err = primitive.WriteString(event.GetEventType(), dest); err != nil {
		return fmt.Errorf("cannot write EVENT type: %v", err)
	}
	switch event.GetEventType() {
	case primitive.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.SchemaChangeEvent, got %T", msg)
		}
		if err = primitive.CheckSchemaChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitive.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.ChangeType: %w", err)
		}
		if err = primitive.CheckSchemaChangeTarget(sce.Target); err != nil {
			return err
		} else if err = primitive.WriteString(sce.Target, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.Target: %w", err)
		}
		if sce.Keyspace == "" {
			return errors.New("EVENT SchemaChange: cannot write empty keyspace")
		} else if err = primitive.WriteString(sce.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.Keyspace: %w", err)
		}
		switch sce.Target {
		case primitive.SchemaChangeTargetKeyspace:
		case primitive.SchemaChangeTargetTable:
			fallthrough
		case primitive.SchemaChangeTargetType:
			if sce.Object == "" {
				return errors.New("EVENT SchemaChange: cannot write empty object")
			} else if err = primitive.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
			}
		case primitive.SchemaChangeTargetAggregate:
			fallthrough
		case primitive.SchemaChangeTargetFunction:
			if version < primitive.ProtocolVersion4 {
				return fmt.Errorf("%s schema change targets are not supported in protocol version %d", sce.Target, version)
			}
			if sce.Keyspace == "" {
				return errors.New("EVENT SchemaChange: cannot write empty object")
			} else if err = primitive.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
			}
			if err = primitive.WriteStringList(sce.Arguments, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Arguments: %w", err)
			}
		default:
			return fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
		return nil
	case primitive.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.StatusChangeEvent, got %T", msg)
		}
		if err = primitive.CheckStatusChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitive.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write StatusChangeEvent.ChangeType: %w", err)
		}
		if err = primitive.WriteInet(sce.Address, dest); err != nil {
			return fmt.Errorf("cannot write StatusChangeEvent.Address: %w", err)
		}
		return nil
	case primitive.EventTypeTopologyChange:
		tce, ok := msg.(*TopologyChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.TopologyChangeEvent, got %T", msg)
		}
		if err = primitive.CheckTopologyChangeType(tce.ChangeType); err != nil {
			return err
		} else if err = primitive.WriteString(tce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.ChangeType: %w", err)
		}
		if err = primitive.WriteInet(tce.Address, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.Address: %w", err)
		}
		return nil
	}
	return errors.New("unknown EVENT type: " + event.GetEventType())
}

func (c *EventCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (length int, err error) {
	event, ok := msg.(Event)
	if !ok {
		return -1, fmt.Errorf("expected message.Event, got %T", msg)
	}
	length = primitive.LengthOfString(event.GetEventType())
	switch event.GetEventType() {
	case primitive.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.SchemaChangeEvent, got %T", msg)
		}
		length += primitive.LengthOfString(sce.ChangeType)
		length += primitive.LengthOfString(sce.Target)
		length += primitive.LengthOfString(sce.Keyspace)
		switch sce.Target {
		case primitive.SchemaChangeTargetKeyspace:
		case primitive.SchemaChangeTargetTable:
			fallthrough
		case primitive.SchemaChangeTargetType:
			length += primitive.LengthOfString(sce.Object)
		case primitive.SchemaChangeTargetAggregate:
			fallthrough
		case primitive.SchemaChangeTargetFunction:
			length += primitive.LengthOfString(sce.Object)
			length += primitive.LengthOfStringList(sce.Arguments)
		default:
			return -1, fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
		return length, nil
	case primitive.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.StatusChangeEvent, got %T", msg)
		}
		length += primitive.LengthOfString(sce.ChangeType)
		inetLength, err := primitive.LengthOfInet(sce.Address)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of StatusChangeEvent.Address: %w", err)
		}
		length += inetLength
		return length, nil
	case primitive.EventTypeTopologyChange:
		tce, ok := msg.(*TopologyChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.TopologyChangeEvent, got %T", msg)
		}
		length += primitive.LengthOfString(tce.ChangeType)
		inetLength, err := primitive.LengthOfInet(tce.Address)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of TopologyChangeEvent.Address: %w", err)
		}
		length += inetLength
		return length, nil
	}
	return -1, errors.New("unknown EVENT type: " + event.GetEventType())
}

func (c *EventCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (Message, error) {
	eventType, err := primitive.ReadString(source)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case primitive.EventTypeSchemaChange:
		sce := &SchemaChangeEvent{}
		if sce.ChangeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.ChangeType: %w", err)
		}
		if sce.Target, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.Target: %w", err)
		}
		if sce.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.Keyspace: %w", err)
		}
		switch sce.Target {
		case primitive.SchemaChangeTargetKeyspace:
		case primitive.SchemaChangeTargetTable:
			fallthrough
		case primitive.SchemaChangeTargetType:
			if sce.Object, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
		case primitive.SchemaChangeTargetAggregate:
			fallthrough
		case primitive.SchemaChangeTargetFunction:
			if version < primitive.ProtocolVersion4 {
				return nil, fmt.Errorf("%s schema change targets are not supported in protocol version %d", sce.Target, version)
			}
			if sce.Object, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
			if sce.Arguments, err = primitive.ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Arguments: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
		return sce, nil
	case primitive.EventTypeStatusChange:
		sce := &StatusChangeEvent{}
		if sce.ChangeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.ChangeType: %w", err)
		}
		if sce.Address, err = primitive.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.Address: %w", err)
		}
		return sce, nil
	case primitive.EventTypeTopologyChange:
		tce := &TopologyChangeEvent{}
		if tce.ChangeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.ChangeType: %w", err)
		}
		if tce.Address, err = primitive.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.Address: %w", err)
		}
		return tce, nil
	}
	return nil, errors.New("unknown EVENT type: " + eventType)
}

func (c *EventCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeEvent
}
