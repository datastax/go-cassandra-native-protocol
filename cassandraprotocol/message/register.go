package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Register struct {
	EventTypes []primitives.EventType
}

func (m *Register) IsResponse() bool {
	return false
}

func (m *Register) GetOpCode() primitives.OpCode {
	return primitives.OpCodeRegister
}

func (m *Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}

type RegisterCodec struct{}

func (c *RegisterCodec) Encode(msg Message, dest io.Writer, _ primitives.ProtocolVersion) error {
	register, ok := msg.(*Register)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	if len(register.EventTypes) == 0 {
		return errors.New("REGISTER messages must have at least one event type")
	}
	for _, eventType := range register.EventTypes {
		if err := primitives.CheckEventType(eventType); err != nil {
			return err
		}
	}
	return primitives.WriteStringList(register.EventTypes, dest)
}

func (c *RegisterCodec) EncodedLength(msg Message, _ primitives.ProtocolVersion) (int, error) {
	register, ok := msg.(*Register)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	return primitives.LengthOfStringList(register.EventTypes), nil
}

func (c *RegisterCodec) Decode(source io.Reader, _ primitives.ProtocolVersion) (Message, error) {
	if eventTypes, err := primitives.ReadStringList(source); err != nil {
		return nil, err
	} else {
		for _, eventType := range eventTypes {
			if err := primitives.CheckEventType(eventType); err != nil {
				return nil, err
			}
		}
		return &Register{EventTypes: eventTypes}, nil
	}
}

func (c *RegisterCodec) GetOpCode() primitives.OpCode {
	return primitives.OpCodeRegister
}
