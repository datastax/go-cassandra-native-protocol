package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Query struct {
	Query   string
	Options *QueryOptions
}

func (q *Query) String() string {
	return fmt.Sprintf("QUERY %s", q.Query)
}

func (q *Query) IsResponse() bool {
	return false
}

func (q *Query) GetOpCode() primitive.OpCode {
	return primitive.OpCodeQuery
}

type queryCodec struct{}

func (c *queryCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) error {
	query, ok := msg.(*Query)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Query, got %T", msg))
	}
	if query.Query == "" {
		return errors.New("cannot write QUERY empty query string")
	} else if err := primitive.WriteLongString(query.Query, dest); err != nil {
		return fmt.Errorf("cannot write QUERY query string: %w", err)
	}
	options := query.Options
	if options == nil {
		options = &QueryOptions{} // use defaults if nil provided
	}
	if err := EncodeQueryOptions(options, dest, version); err != nil {
		return fmt.Errorf("cannot write QUERY options: %w", err)
	}
	return nil
}

func (c *queryCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (int, error) {
	query, ok := msg.(*Query)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Query, got %T", msg))
	}
	lengthOfQuery := primitive.LengthOfLongString(query.Query)
	lengthOfQueryOptions, err := LengthOfQueryOptions(query.Options, version)
	if err != nil {
		return -1, fmt.Errorf("cannot compute size of QUERY message: %w", err)
	}
	return lengthOfQuery + lengthOfQueryOptions, nil
}

func (c *queryCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (Message, error) {
	if query, err := primitive.ReadLongString(source); err != nil {
		return nil, err
	} else if query == "" {
		return nil, fmt.Errorf("cannot read QUERY empty query string")
	} else if options, err := DecodeQueryOptions(source, version); err != nil {
		return nil, err
	} else {
		return &Query{Query: query, Options: options}, nil
	}
}

func (c *queryCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeQuery
}
