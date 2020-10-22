package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
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

func (c *QueryCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	query, ok := msg.(*Query)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Query, got %T", msg))
	}
	err = primitives.WriteLongString(query.Query, dest)
	if err == nil {
		err = EncodeQueryOptions(query.Options, dest, version)
	}
	return err
}

func (c *QueryCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	query, ok := msg.(*Query)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Query, got %T", msg))
	}
	lengthOfQuery := primitives.LengthOfLongString(query.Query)
	lengthOfQueryOptions, err := LengthOfQueryOptions(query.Options, version)
	if err != nil {
		return -1, fmt.Errorf("cannot compute size of QUERY message: %w", err)
	}
	return lengthOfQuery + lengthOfQueryOptions, nil
}

func (c *QueryCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	var query string
	query, err = primitives.ReadLongString(source)
	if err == nil {
		var options *QueryOptions
		options, err = DecodeQueryOptions(source, version)
		if err == nil {
			return &Query{Query: query, Options: options}, nil
		}
	}
	return nil, err
}

func (c *QueryCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeQuery
}
