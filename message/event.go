// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"errors"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type Event interface {
	Message
	GetEventType() primitive.EventType
}

// SCHEMA CHANGE EVENT

// SchemaChangeEvent is a response sent when a schema change event occurs.
// Note: this struct is identical to SchemaChangeResult.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type SchemaChangeEvent struct {
	// The schema change type.
	ChangeType primitive.SchemaChangeType
	// The schema change target, that is, the kind of schema object affected by the change.
	Target primitive.SchemaChangeTarget
	// The name of the keyspace affected by the change.
	Keyspace string
	// If the schema object affected by the change is not the keyspace itself, this field contains its name. Otherwise,
	// this field is irrelevant and should be empty.
	Object string
	// If the schema object affected by the change is a function or an aggregate, this field contains its arguments.
	// Otherwise, this field is irrelevant. Valid from protocol version 4 onwards.
	Arguments []string
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

// StatusChangeEvent is a response sent when a node status change event occurs.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
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

// TopologyChangeEvent is a response sent when a topology change event occurs.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type TopologyChangeEvent struct {
	// The topology change type. Note that MOVED_NODE is only valid from protocol version 3 onwards.
	ChangeType primitive.TopologyChangeType
	// The address of the node.
	Address *primitive.Inet
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

type eventCodec struct{}

func (c *eventCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	event, ok := msg.(Event)
	if !ok {
		return fmt.Errorf("expected message.Event, got %T", msg)
	}
	if err = primitive.CheckValidEventType(event.GetEventType()); err != nil {
		return err
	} else if err = primitive.WriteString(string(event.GetEventType()), dest); err != nil {
		return fmt.Errorf("cannot write EVENT type: %v", err)
	}
	switch event.GetEventType() {
	case primitive.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.SchemaChangeEvent, got %T", msg)
		}
		if err = primitive.CheckValidSchemaChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitive.WriteString(string(sce.ChangeType), dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeEvent.ChangeType: %w", err)
		}
		if version >= primitive.ProtocolVersion3 {
			if err = primitive.CheckValidSchemaChangeTarget(sce.Target, version); err != nil {
				return err
			} else if err = primitive.WriteString(string(sce.Target), dest); err != nil {
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
				if sce.Keyspace == "" {
					return errors.New("EVENT SchemaChange: cannot write empty object")
				} else if err = primitive.WriteString(sce.Object, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
				}
				if err = primitive.WriteStringList(sce.Arguments, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeEvent.Arguments: %w", err)
				}
			}
		} else {
			if err = primitive.CheckValidSchemaChangeTarget(sce.Target, version); err != nil {
				return err
			}
			if sce.Keyspace == "" {
				return errors.New("EVENT SchemaChange: cannot write empty keyspace")
			} else if err = primitive.WriteString(sce.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Keyspace: %w", err)
			}
			switch sce.Target {
			case primitive.SchemaChangeTargetKeyspace:
				if sce.Object != "" {
					return errors.New("EVENT SchemaChange: table must be empty for keyspace targets")
				} else if err = primitive.WriteString("", dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
				}
			case primitive.SchemaChangeTargetTable:
				if sce.Object == "" {
					return errors.New("EVENT SchemaChange: cannot write empty table")
				} else if err = primitive.WriteString(sce.Object, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
				}
			}
		}
		return nil
	case primitive.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return fmt.Errorf("expected *message.StatusChangeEvent, got %T", msg)
		}
		if err = primitive.CheckValidStatusChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitive.WriteString(string(sce.ChangeType), dest); err != nil {
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
		if err = primitive.CheckValidTopologyChangeType(tce.ChangeType, version); err != nil {
			return err
		} else if err = primitive.WriteString(string(tce.ChangeType), dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.ChangeType: %w", err)
		}
		if err = primitive.WriteInet(tce.Address, dest); err != nil {
			return fmt.Errorf("cannot write TopologyChangeEvent.Address: %w", err)
		}
		return nil
	}
	return fmt.Errorf("unknown EVENT type: %v", event.GetEventType())
}

func (c *eventCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (length int, err error) {
	event, ok := msg.(Event)
	if !ok {
		return -1, fmt.Errorf("expected message.Event, got %T", msg)
	}
	length = primitive.LengthOfString(string(event.GetEventType()))
	switch event.GetEventType() {
	case primitive.EventTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.SchemaChangeEvent, got %T", msg)
		}
		length += primitive.LengthOfString(string(sce.ChangeType))
		if err = primitive.CheckValidSchemaChangeTarget(sce.Target, version); err != nil {
			return -1, err
		}
		if version >= primitive.ProtocolVersion3 {
			length += primitive.LengthOfString(string(sce.Target))
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
			}
		} else {
			length += primitive.LengthOfString(sce.Keyspace)
			length += primitive.LengthOfString(sce.Object)
		}
		return length, nil
	case primitive.EventTypeStatusChange:
		sce, ok := msg.(*StatusChangeEvent)
		if !ok {
			return -1, fmt.Errorf("expected *message.StatusChangeEvent, got %T", msg)
		}
		length += primitive.LengthOfString(string(sce.ChangeType))
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
		length += primitive.LengthOfString(string(tce.ChangeType))
		inetLength, err := primitive.LengthOfInet(tce.Address)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of TopologyChangeEvent.Address: %w", err)
		}
		length += inetLength
		return length, nil
	}
	return -1, fmt.Errorf("unknown EVENT type: %v", event.GetEventType())
}

func (c *eventCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (Message, error) {
	eventType, err := primitive.ReadString(source)
	if err != nil {
		return nil, err
	}
	switch primitive.EventType(eventType) {
	case primitive.EventTypeSchemaChange:
		sce := &SchemaChangeEvent{}
		var changeType string
		if changeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeEvent.ChangeType: %w", err)
		}
		sce.ChangeType = primitive.SchemaChangeType(changeType)
		if version >= primitive.ProtocolVersion3 {
			var target string
			if target, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Target: %w", err)
			}
			sce.Target = primitive.SchemaChangeTarget(target)
			if err = primitive.CheckValidSchemaChangeTarget(sce.Target, version); err != nil {
				return nil, err
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
				if sce.Object, err = primitive.ReadString(source); err != nil {
					return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
				}
				if sce.Arguments, err = primitive.ReadStringList(source); err != nil {
					return nil, fmt.Errorf("cannot read SchemaChangeEvent.Arguments: %w", err)
				}
			default:
				return nil, fmt.Errorf("unknown schema change target: %v", sce.Target)
			}
		} else {
			if sce.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Keyspace: %w", err)
			}
			if sce.Object, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
			if sce.Object == "" {
				sce.Target = primitive.SchemaChangeTargetKeyspace
			} else {
				sce.Target = primitive.SchemaChangeTargetTable
			}
		}
		return sce, nil
	case primitive.EventTypeStatusChange:
		sce := &StatusChangeEvent{}
		var changeType string
		if changeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.ChangeType: %w", err)
		}
		sce.ChangeType = primitive.StatusChangeType(changeType)
		if sce.Address, err = primitive.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read StatusChangeEvent.Address: %w", err)
		}
		return sce, nil
	case primitive.EventTypeTopologyChange:
		tce := &TopologyChangeEvent{}
		var changeType string
		if changeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.ChangeType: %w", err)
		}
		tce.ChangeType = primitive.TopologyChangeType(changeType)
		if tce.Address, err = primitive.ReadInet(source); err != nil {
			return nil, fmt.Errorf("cannot read TopologyChangeEvent.Address: %w", err)
		}
		return tce, nil
	}
	return nil, errors.New("unknown EVENT type: " + eventType)
}

func (c *eventCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeEvent
}
