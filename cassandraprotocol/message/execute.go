package message

import (
	"encoding/hex"
	"go-cassandra-native-protocol/cassandraprotocol"
)

type Execute struct {
	QueryId          []byte
	ResultMetadataId []byte
	Options          *QueryOptions
}

func (m *Execute) IsResponse() bool {
	return false
}

func (m *Execute) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeExecute
}

func (m *Execute) String() string {
	return "EXECUTE " + hex.EncodeToString(m.QueryId)
}
