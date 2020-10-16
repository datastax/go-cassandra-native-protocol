package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type RegisterCodec struct{}

func (c RegisterCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	register := msg.(*message.Register)
	_, err := primitives.WriteStringList(register.EventTypes, dest)
	return err
}

func (c RegisterCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	register := msg.(*message.Register)
	return primitives.LengthOfStringList(register.EventTypes), nil
}

func (c RegisterCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	eventTypes, _, err := primitives.ReadStringList(source)
	if err != nil {
		return nil, err
	}
	return message.NewRegister(eventTypes), nil
}
