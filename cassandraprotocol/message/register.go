package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Register struct {
	EventTypes []cassandraprotocol.EventType
}

func NewRegister(eventTypes []cassandraprotocol.EventType) *Register {
	return &Register{EventTypes: eventTypes}
}

func (m Register) IsResponse() bool {
	return false
}

func (m Register) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeRegister
}

func (m Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}

type RegisterCodec struct{}

func (c RegisterCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	register := msg.(*Register)
	_, err := primitives.WriteStringList(register.EventTypes, dest)
	return err
}

func (c RegisterCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	register := msg.(*Register)
	return primitives.LengthOfStringList(register.EventTypes), nil
}

func (c RegisterCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	eventTypes, _, err := primitives.ReadStringList(source)
	if err != nil {
		return nil, err
	}
	return NewRegister(eventTypes), nil
}
