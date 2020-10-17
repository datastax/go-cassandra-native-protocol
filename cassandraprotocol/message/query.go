package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Query struct {
	Query   string
	Options *QueryOptions
}

func (q Query) IsResponse() bool {
	return false
}

func (q Query) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeQuery
}

type QueryCodec struct {
}

func (c *QueryCodec) Encode(msg Message, dest []byte, version cassandraprotocol.ProtocolVersion) (err error) {
	query := msg.(*Query)
	dest, err = primitives.WriteLongString(query.Query, dest)
	if err == nil {
		_, err = EncodeQueryOptions(query.Options, dest, version)
	}
	return err
}

func (q QueryCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	query := msg.(*Query)
	lengthOfQuery := primitives.LengthOfLongString(query.Query)
	lengthOfQueryOptions, err := LengthOfQueryOptions(query.Options, version)
	if err != nil {
		return -1, fmt.Errorf("cannot compute size of QUERY message: %w", err)
	}
	return lengthOfQuery + lengthOfQueryOptions, nil
}

func (q QueryCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	var query string
	query, source, err = primitives.ReadLongString(source)
	if err == nil {
		var options *QueryOptions
		options, source, err = DecodeQueryOptions(source, version)
		if err == nil {
			return Query{Query: query, Options: options}, nil
		}
	}
	return nil, err
}
