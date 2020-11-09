package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Register struct {
	EventTypes []primitive.EventType
}

func (m *Register) IsResponse() bool {
	return false
}

func (m *Register) GetOpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}

func (m *Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}

type registerCodec struct{}

func (c *registerCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	register, ok := msg.(*Register)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	if len(register.EventTypes) == 0 {
		return errors.New("REGISTER messages must have at least one event type")
	}
	for _, eventType := range register.EventTypes {
		if err := primitive.CheckValidEventType(eventType); err != nil {
			return err
		}
	}
	return primitive.WriteStringList(asStringList(register.EventTypes), dest)
}

func (c *registerCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	register, ok := msg.(*Register)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	return primitive.LengthOfStringList(asStringList(register.EventTypes)), nil
}

func (c *registerCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if eventTypes, err := primitive.ReadStringList(source); err != nil {
		return nil, err
	} else {
		for _, eventType := range eventTypes {
			if err := primitive.CheckValidEventType(primitive.EventType(eventType)); err != nil {
				return nil, err
			}
		}
		return &Register{EventTypes: fromStringList(eventTypes)}, nil
	}
}

func (c *registerCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}

func asStringList(eventTypes []primitive.EventType) []string {
	strings := make([]string, len(eventTypes))
	for i, eventType := range eventTypes {
		strings[i] = string(eventType)
	}
	return strings
}

func fromStringList(strings []string) []primitive.EventType {
	eventTypes := make([]primitive.EventType, len(strings))
	for i, eventType := range strings {
		eventTypes[i] = primitive.EventType(eventType)
	}
	return eventTypes
}
