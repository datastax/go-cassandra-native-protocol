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

type RegisterCodec struct{}

func (c *RegisterCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	register, ok := msg.(*Register)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	if len(register.EventTypes) == 0 {
		return errors.New("REGISTER messages must have at least one event type")
	}
	for _, eventType := range register.EventTypes {
		if err := primitive.CheckEventType(eventType); err != nil {
			return err
		}
	}
	return primitive.WriteStringList(register.EventTypes, dest)
}

func (c *RegisterCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	register, ok := msg.(*Register)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	return primitive.LengthOfStringList(register.EventTypes), nil
}

func (c *RegisterCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if eventTypes, err := primitive.ReadStringList(source); err != nil {
		return nil, err
	} else {
		for _, eventType := range eventTypes {
			if err := primitive.CheckEventType(eventType); err != nil {
				return nil, err
			}
		}
		return &Register{EventTypes: eventTypes}, nil
	}
}

func (c *RegisterCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}
