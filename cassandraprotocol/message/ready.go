package message

import "go-cassandra-native-protocol/cassandraprotocol"

type Ready struct {
}

func (m Ready) IsResponse() bool {
	return false
}

func (m Ready) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeReady
}

func (m Ready) String() string {
	return "READY"
}

type ReadyCodec struct{}

func (c ReadyCodec) Encode(_ Message, _ []byte, _ cassandraprotocol.ProtocolVersion) error {
	return nil
}

func (c ReadyCodec) EncodedSize(_ Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c ReadyCodec) Decode(_ []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	return &Ready{}, nil
}
