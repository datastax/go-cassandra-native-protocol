package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Register struct {
	EventTypes []cassandraprotocol.EventType
}

func (m *Register) IsResponse() bool {
	return false
}

func (m *Register) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeRegister
}

func (m *Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}

type RegisterCodec struct{}

func (c *RegisterCodec) Encode(msg Message, dest io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	register, ok := msg.(*Register)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	return primitives.WriteStringList(register.EventTypes, dest)
}

func (c *RegisterCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	register, ok := msg.(*Register)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	return primitives.LengthOfStringList(register.EventTypes), nil
}

func (c *RegisterCodec) Decode(source io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	if eventTypes, err := primitives.ReadStringList(source); err != nil {
		return nil, err
	} else {
		return &Register{EventTypes: eventTypes}, nil
	}
}

func (c *RegisterCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeRegister
}
