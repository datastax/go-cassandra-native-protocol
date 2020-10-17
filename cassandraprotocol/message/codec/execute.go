package codec

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type ExecuteCodec struct {
}

func (c ExecuteCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) (err error) {
	execute := msg.(*message.Execute)
	if dest, err = primitives.WriteShortBytes(execute.QueryId, dest); err != nil {
		return fmt.Errorf("cannot write EXECUTE query id: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		if dest, err = primitives.WriteShortBytes(execute.ResultMetadataId, dest); err != nil {
			return fmt.Errorf("cannot write EXECUTE result metadata id: %w", err)
		}
	}
	if dest, err = EncodeQueryOptions(execute.Options, dest, version); err != nil {
		return fmt.Errorf("cannot write EXECUTE query id: %w", err)
	}
	return
}

func (c ExecuteCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (size int, err error) {
	execute := msg.(*message.Execute)
	size += primitives.LengthOfShortBytes(execute.QueryId)
	if version >= cassandraprotocol.ProtocolVersion5 {
		size += primitives.LengthOfShortBytes(execute.ResultMetadataId)
	}
	if lengthOfQueryOptions, err := LengthOfQueryOptions(execute.Options, version); err == nil {
		return size + lengthOfQueryOptions, nil
	} else {
		return -1, fmt.Errorf("cannot compute size EXECUTE query options: %w", err)
	}
}

func (c ExecuteCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (msg message.Message, err error) {
	var queryId []byte
	if queryId, source, err = primitives.ReadShortBytes(source); err != nil {
		return nil, fmt.Errorf("cannot read EXECUTE query id: %w", err)
	}
	var resultMetadataId []byte
	if version >= cassandraprotocol.ProtocolVersion5 {
		if resultMetadataId, source, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read EXECUTE result metadata id: %w", err)
		}
	}
	var options *message.QueryOptions
	if options, source, err = DecodeQueryOptions(source, version); err != nil {
		return nil, fmt.Errorf("cannot read EXECUTE query options: %w", err)
	}
	return &message.Execute{
		QueryId:          queryId,
		ResultMetadataId: resultMetadataId,
		Options:          options,
	}, nil
}
