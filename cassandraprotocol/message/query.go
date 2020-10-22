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

func (c *QueryCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) error {
	query, ok := msg.(*Query)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Query, got %T", msg))
	}
	if query.Query == "" {
		return errors.New("QUERY missing query string")
	} else if err := primitives.WriteLongString(query.Query, dest); err != nil {
		return fmt.Errorf("cannot write QUERY query string: %w", err)
	}
	options := query.Options
	if options == nil {
		options = NewQueryOptions() // use defaults if nil provided
	}
	if err := EncodeQueryOptions(query.Options, dest, version); err != nil {
		return fmt.Errorf("cannot write QUERY options: %w", err)
	}
	return nil
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

func (c *QueryCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (Message, error) {
	if query, err := primitives.ReadLongString(source); err != nil {
		return nil, err
	} else if query == "" {
		return nil, fmt.Errorf("QUERY missing query string")
	} else if options, err := DecodeQueryOptions(source, version); err != nil {
		return nil, err
	} else {
		return &Query{Query: query, Options: options}, nil
	}
}

func (c *QueryCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeQuery
}
