package codec

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type EventCodec struct{}

func (c EventCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	event := msg.(*message.Event)
	var err error
	dest, err = primitive.WriteString(event.Type, dest)
	if err != nil {
		return err
	}
	switch event.Type {
	case cassandraprotocol.EventTypeSchemaChange:
		sce, ok := msg.(*message.SchemaChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected SchemaChangeEvent struct, got %T", sce))
		}
		dest, err = primitive.WriteString(sce.ChangeType, dest)
		if err != nil {
			return err
		}
		dest, err = primitive.WriteString(sce.Target, dest)
		if err != nil {
			return err
		}
		dest, err = primitive.WriteString(sce.Keyspace, dest)
		if err != nil {
			return err
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			dest, err = primitive.WriteString(sce.Object, dest)
			if err != nil {
				return err
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			dest, err = primitive.WriteString(sce.Object, dest)
			if err != nil {
				return err
			}
			dest, err = primitive.WriteStringList(sce.Arguments, dest)
			if err != nil {
				return err
			}
		default:
			return errors.New(fmt.Sprintf("unknown schema change target: " + sce.Target))
		}
		return nil
	case cassandraprotocol.EventTypeStatusChange:
		sce, ok := msg.(*message.StatusChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected StatusChangeEvent struct, got %T", sce))
		}
		dest, err = primitive.WriteString(sce.ChangeType, dest)
		if err != nil {
			return err
		}
		dest, err = primitive.WriteInet(sce.Address, dest)
		if err != nil {
			return err
		}
		return nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce, ok := msg.(*message.TopologyChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected TopologyChangeEvent struct, got %T", tce))
		}
		dest, err = primitive.WriteString(tce.ChangeType, dest)
		if err != nil {
			return err
		}
		dest, err = primitive.WriteInet(tce.Address, dest)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("unknown event type: " + event.Type)
}

func (c EventCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	event := msg.(*message.Event)
	size := primitive.SizeOfString(event.Type)
	switch event.Type {
	case cassandraprotocol.EventTypeSchemaChange:
		sce, ok := msg.(*message.SchemaChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected SchemaChangeEvent struct, got %T", sce))
		}
		size += primitive.SizeOfString(sce.ChangeType)
		size += primitive.SizeOfString(sce.Target)
		size += primitive.SizeOfString(sce.Keyspace)
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			size += primitive.SizeOfString(sce.Object)
			break
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return -1, errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			size += primitive.SizeOfString(sce.Object)
			size += primitive.SizeOfStringList(sce.Arguments)
		default:
			return -1, errors.New(fmt.Sprintf("unknown schema change target: " + sce.Target))
		}
		return size, nil
	case cassandraprotocol.EventTypeStatusChange:
		sce, ok := msg.(*message.StatusChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected StatusChangeEvent struct, got %T", sce))
		}
		size += primitive.SizeOfString(sce.ChangeType)
		size += primitive.SizeOfInet(sce.Address)
		return size, nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce, ok := msg.(*message.TopologyChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected TopologyChangeEvent struct, got %T", tce))
		}
		size += primitive.SizeOfString(tce.ChangeType)
		size += primitive.SizeOfInet(tce.Address)
		return size, nil
	}
	return -1, errors.New("unknown event type: " + event.Type)
}

func (c EventCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	eventType, _, err := primitive.ReadString(source)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case cassandraprotocol.EventTypeSchemaChange:
		sce := &message.SchemaChangeEvent{Event: message.Event{Type: eventType}}
		sce.ChangeType, source, err = primitive.ReadString(source)
		if err != nil {
			return nil, err
		}
		sce.Target, source, err = primitive.ReadString(source)
		if err != nil {
			return nil, err
		}
		sce.Keyspace, source, err = primitive.ReadString(source)
		if err != nil {
			return nil, err
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			sce.Object, source, err = primitive.ReadString(source)
			if err != nil {
				return nil, err
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return nil, errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			sce.Object, source, err = primitive.ReadString(source)
			if err != nil {
				return nil, err
			}
			sce.Arguments, source, err = primitive.ReadStringList(source)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(fmt.Sprintf("unknown schema change target: " + sce.Target))
		}
		return sce, nil
	case cassandraprotocol.EventTypeStatusChange:
		sce := &message.StatusChangeEvent{Event: message.Event{Type: eventType}}
		sce.ChangeType, source, err = primitive.ReadString(source)
		if err != nil {
			return nil, err
		}
		sce.Address, source, err = primitive.ReadInet(source)
		if err != nil {
			return nil, err
		}
		return sce, nil
	case cassandraprotocol.EventTypeTopologyChange:
		tce := &message.TopologyChangeEvent{Event: message.Event{Type: eventType}}
		tce.ChangeType, source, err = primitive.ReadString(source)
		if err != nil {
			return nil, err
		}
		tce.Address, source, err = primitive.ReadInet(source)
		if err != nil {
			return nil, err
		}
		return tce, nil
	}
	return nil, errors.New("unknown event type: " + eventType)
}
