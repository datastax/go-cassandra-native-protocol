package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type StartupCodec struct{}

func (c StartupCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeStartup
}

func (c StartupCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	startup := msg.(*message.Startup)
	_, err := WriteStringMap(startup.Options, dest)
	return err
}

func (c StartupCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	startup := msg.(*message.Startup)
	return SizeOfStringMap(startup.Options), nil
}

func (c StartupCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	options, _, err := ReadStringMap(source)
	if err != nil {
		return nil, err
	}
	return message.NewStartupWithOptions(options), nil
}
