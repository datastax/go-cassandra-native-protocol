// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"errors"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Query is a request that executes a CQL query.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
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
	if err := primitive.WriteLongString(query.Query, dest); err != nil {
		return fmt.Errorf("cannot write QUERY query string: %w", err)
	}
	if err := EncodeQueryOptions(query.Options, dest, version); err != nil {
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
	} else if options, err := DecodeQueryOptions(source, version); err != nil {
		return nil, err
	} else {
		return &Query{Query: query, Options: options}, nil
	}
}

func (c *queryCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeQuery
}
