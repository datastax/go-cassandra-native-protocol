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
	"bytes"
	"errors"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryCodec_Encode(t *testing.T) {
	codec := &queryCodec{}
	// tests for version 2
	t.Run(primitive.ProtocolVersion2.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 2,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
				},
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				nil,
			},
			{
				"missing query",
				&Query{},
				nil,
				errors.New("cannot write QUERY empty query string"),
			},
			{
				"not a query",
				&Options{},
				nil,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion2)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version 3
	t.Run(primitive.ProtocolVersion3.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 2,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
				},
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				nil,
			},
			{
				"missing query",
				&Query{},
				nil,
				errors.New("cannot write QUERY empty query string"),
			},
			{
				"not a query",
				&Options{},
				nil,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion3)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 4
	t.Run(primitive.ProtocolVersion4.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    Message
			expected []byte
			err      error
		}{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				nil,
			},
			{
				"missing query",
				&Query{},
				nil,
				errors.New("cannot write QUERY empty query string"),
			},
			{
				"not a query",
				&Options{},
				nil,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion4)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(primitive.ProtocolVersion5.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with keyspace and now-in-seconds",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: &primitive.NillableInt32{Value: 123},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b0000_0001,    // flags (keyspace)
					0b1000_0000,    // flags (now in seconds)
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 123, // now in seconds
				},
				nil,
			},
			{
				"query with positional values, keyspace and now-in-seconds",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: &primitive.NillableInt32{Value: 123},
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0000, // flags
					0b0000_0000, // flags
					0b0000_0001, // flags
					0b1000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 123, // now in seconds
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(primitive.ProtocolVersionDse1.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    Message
			expected []byte
			err      error
		}{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, 0, 0, 0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:             primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:            true,
						PageSize:                100,
						PageSizeInBytes:         true,
						PagingState:             []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency:       &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:        &primitive.NillableInt64{Value: 123},
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b1100_0000, 0, 0, 0b0011_1110, // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					0, 0, 0, 50, // max pages
					0, 0, 0, 10, // pages per sec
				},
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, 0, 0, 0b0000_0001, // flags
					0, 3, // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, 0, 0, 0b0100_0001, // flags
					0, 1, // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				nil,
			},
			{
				"missing query",
				&Query{},
				nil,
				errors.New("cannot write QUERY empty query string"),
			},
			{
				"not a query",
				&Options{},
				nil,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(primitive.ProtocolVersionDse2.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with keyspace and continuous options",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:                "ks1",
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10, NextPages: 20},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b1000_0000,    // flags (cont. paging)
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b1000_0000,    // flags (keyspace)
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 50, // max pages
					0, 0, 0, 10, // pages per sec
					0, 0, 0, 20, // next pages
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:             primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:            true,
						PageSize:                100,
						PageSizeInBytes:         true,
						PagingState:             []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency:       &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:        &primitive.NillableInt64{Value: 123},
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10, NextPages: 20},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b1100_0000, 0, 0, 0b0011_1110, // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					0, 0, 0, 50, // max pages
					0, 0, 0, 10, // pages per sec
					0, 0, 0, 20, // next pages
				},
				nil,
			},
			{
				"query with positional values and keyspace",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace: "ks1",
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0000, // flags
					0b0000_0000, // flags
					0b0000_0000, // flags
					0b1000_0001, // flags (keyspace | values)
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
					0, 3, k, s, _1, // keyspace
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestQueryCodec_EncodedLength(t *testing.T) {
	codec := &queryCodec{}
	// tests for version 2
	t.Run(primitive.ProtocolVersion2.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfInt + // page size
					primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitive.LengthOfShort + // serial consistency
					primitive.LengthOfLong, // default timestamp
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt, // value 2
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfString("col1") + // name 1
					primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
				nil,
			},
			{
				"not a query",
				&Options{},
				-1,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version 3
	t.Run(primitive.ProtocolVersion3.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfInt + // page size
					primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitive.LengthOfShort + // serial consistency
					primitive.LengthOfLong, // default timestamp
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt, // value 2
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfString("col1") + // name 1
					primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
				nil,
			},
			{
				"not a query",
				&Options{},
				-1,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion3)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 4
	t.Run(primitive.ProtocolVersion4.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    Message
			expected int
			err      error
		}{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfInt + // page size
					primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitive.LengthOfShort + // serial consistency
					primitive.LengthOfLong, // default timestamp
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt, // value 3
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfString("col1") + // name 1
					primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
				nil,
			},
			{
				"not a query",
				&Options{},
				-1,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion4)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(primitive.ProtocolVersion5.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with keyspace and now-in-seconds",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: &primitive.NillableInt32{Value: 123},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfString("ks1") + // keyspace
					primitive.LengthOfInt, // new in seconds
				nil,
			},
			{
				"query with positional values, keyspace and now-in-seconds",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: &primitive.NillableInt32{Value: 123},
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt + // value 3
					primitive.LengthOfString("ks1") + // keyspace
					primitive.LengthOfInt, // new in seconds
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(primitive.ProtocolVersionDse1.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    Message
			expected int
			err      error
		}{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:             primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:            true,
						PageSize:                100,
						PagingState:             []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency:       &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:        &primitive.NillableInt64{Value: 123},
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfInt + // page size
					primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitive.LengthOfShort + // serial consistency
					primitive.LengthOfLong + // default timestamp
					primitive.LengthOfInt*2, // cont. paging options
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt, // value 3
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfString("col1") + // name 1
					primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
				nil,
			},
			{
				"not a query",
				&Options{},
				-1,
				errors.New("expected *message.Query, got *message.Options"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(primitive.ProtocolVersionDse2.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with keyspace",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:                "ks1",
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10, NextPages: 20},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfString("ks1") + // keyspace
					primitive.LengthOfInt*3, // cont. paging options
				nil,
			},
			{
				"query with positional values and keyspace",
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace: "ks1",
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				primitive.LengthOfLongString("SELECT") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt + // value 3
					primitive.LengthOfString("ks1"), // keyspace
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestQueryCodec_Decode(t *testing.T) {
	codec := &queryCodec{}
	// tests for version 2
	t.Run(primitive.ProtocolVersion2.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with default options",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				nil,
			},
			{
				"query with custom options and no values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 2,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
						},
					},
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 0, // consistency level
					0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersion2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version 3
	t.Run(primitive.ProtocolVersion3.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with default options",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				nil,
			},
			{
				"query with custom options and no values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 2,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
						},
					},
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 0, // consistency level
					0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersion3)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 4
	t.Run(primitive.ProtocolVersion4.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected Message
			err      error
		}{
			{
				"query with default options",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				nil,
			},
			{
				"query with custom options and no values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 0, // consistency level
					0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersion4)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(primitive.ProtocolVersion5.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with keyspace and now-in-seconds",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b0000_0001,    // flags (keyspace)
					0b1000_0000,    // flags (now in seconds)
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 123, // now in seconds
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: &primitive.NillableInt32{Value: 123},
					},
				},
				nil,
			},
			{
				"query with positional values, keyspace and now-in-seconds",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0000, // flags
					0b0000_0000, // flags
					0b0000_0001, // flags
					0b1000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 123, // now in seconds
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: &primitive.NillableInt32{Value: 123},
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(primitive.ProtocolVersionDse1.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected Message
			err      error
		}{
			{
				"query with default options",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, 0, 0, 0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: &QueryOptions{},
				},
				nil,
			},
			{
				"query with custom options and no values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b1100_0000, 0, 0, 0b0011_1110, // flags (cont. paging and page size bytes)
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					0, 0, 0, 50, // max pages
					0, 0, 0, 10, // pages per sec
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:             primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:            true,
						PageSize:                100,
						PageSizeInBytes:         true,
						PagingState:             []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency:       &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:        &primitive.NillableInt64{Value: 123},
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10},
					},
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, 0, 0, 0b0000_0001, // flags
					0, 3, // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0, 0, 0, 0b0100_0001, // flags
					0, 1, // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 0, // consistency level
					0, 0, 0, 0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(primitive.ProtocolVersionDse2.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with keyspace and continuous paging",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b1000_0000,    // flags (cont. paging)
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b1000_0000,    // flags (keyspace)
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 50, // max pages
					0, 0, 0, 10, // pages per sec
					0, 0, 0, 20, // next pages
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace:                "ks1",
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10, NextPages: 20},
					},
				},
				nil,
			},
			{
				"query with custom options and no values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 6, // consistency level
					0b1100_0000, 0, 0, 0b0011_1110, // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					0, 0, 0, 50, // max pages
					0, 0, 0, 10, // pages per sec
					0, 0, 0, 20, // next pages
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Consistency:             primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:            true,
						PageSize:                100,
						PageSizeInBytes:         true,
						PagingState:             []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency:       &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:        &primitive.NillableInt64{Value: 123},
						ContinuousPagingOptions: &ContinuousPagingOptions{MaxPages: 50, PagesPerSecond: 10, NextPages: 20},
					},
				},
				nil,
			},
			{
				"query with positional values and keyspace",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 0, // consistency level
					0b0000_0000, // flags
					0b0000_0000, // flags
					0b0000_0000, // flags
					0b1000_0001, // flags (keyspace | values)
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
					0, 3, k, s, _1, // keyspace
				},
				&Query{
					Query: "SELECT",
					Options: &QueryOptions{
						Keyspace: "ks1",
						PositionalValues: []*primitive.Value{
							{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							{
								Type: primitive.ValueTypeNull,
							},
							{
								Type: primitive.ValueTypeUnset,
							},
						},
					},
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}
