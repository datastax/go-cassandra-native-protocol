package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryCodec_Encode(t *testing.T) {
	codec := &QueryCodec{}
	// tests for version 3
	t.Run("version 3", func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
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
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
						),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
				err := codec.Encode(tt.input, dest, cassandraprotocol.ProtocolVersion3)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 4
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersion4), func(t *testing.T) {
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
					Options: NewQueryOptions(),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
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
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
				err := codec.Encode(tt.input, dest, cassandraprotocol.ProtocolVersion4)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(fmt.Sprintf("version %v", cassandraprotocol.ProtocolVersion5), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with keyspace and now-in-seconds",
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123),
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
				err := codec.Encode(tt.input, dest, cassandraprotocol.ProtocolVersion5)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersionDse1), func(t *testing.T) {
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
					Options: NewQueryOptions(),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0, 0, 0, 0, // flags
				},
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPageSizeInBytes(),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
						}),
					),
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
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
				err := codec.Encode(tt.input, dest, cassandraprotocol.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(fmt.Sprintf("version %v", cassandraprotocol.ProtocolVersionDse2), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"query with keyspace and continuous options",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithKeyspace("ks1"),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
							NextPages:      20,
						}),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPageSizeInBytes(),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
							NextPages:      20,
						}),
					),
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
					Options: NewQueryOptions(WithKeyspace("ks1"),
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
				err := codec.Encode(tt.input, dest, cassandraprotocol.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestQueryCodec_EncodedLength(t *testing.T) {
	codec := &QueryCodec{}
	// tests for version 3
	t.Run("version 3", func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with default options",
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfInt + // page size
					primitives.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitives.LengthOfShort + // serial consistency
					primitives.LengthOfLong, // default timestamp
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
						),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitives.LengthOfInt, // value 2
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfString("col1") + // name 1
					primitives.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
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
				actual, err := codec.EncodedLength(tt.input, cassandraprotocol.ProtocolVersion3)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 4
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersion4), func(t *testing.T) {
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
					Options: NewQueryOptions(),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfInt + // page size
					primitives.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitives.LengthOfShort + // serial consistency
					primitives.LengthOfLong, // default timestamp
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitives.LengthOfInt + // value 2
					primitives.LengthOfInt, // value 3
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfString("col1") + // name 1
					primitives.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
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
				actual, err := codec.EncodedLength(tt.input, cassandraprotocol.ProtocolVersion4)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(fmt.Sprintf("version %v", cassandraprotocol.ProtocolVersion5), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with keyspace and now-in-seconds",
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfString("ks1") + // keyspace
					primitives.LengthOfInt, // new in seconds
				nil,
			},
			{
				"query with positional values, keyspace and now-in-seconds",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123),
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitives.LengthOfInt + // value 2
					primitives.LengthOfInt + // value 3
					primitives.LengthOfString("ks1") + // keyspace
					primitives.LengthOfInt, // new in seconds
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, cassandraprotocol.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersionDse1), func(t *testing.T) {
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
					Options: NewQueryOptions(),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt, // flags
				nil,
			},
			{
				"query with custom options and no values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPageSizeInBytes(), // does not count for length
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
						}),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfInt + // page size
					primitives.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitives.LengthOfShort + // serial consistency
					primitives.LengthOfLong + // default timestamp
					primitives.LengthOfInt*2, // cont. paging options
				nil,
			},
			{
				"query with positional values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitives.LengthOfInt + // value 2
					primitives.LengthOfInt, // value 3
				nil,
			},
			{
				"query with named values",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfString("col1") + // name 1
					primitives.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
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
				actual, err := codec.EncodedLength(tt.input, cassandraprotocol.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(fmt.Sprintf("version %v", cassandraprotocol.ProtocolVersionDse2), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"query with keyspace",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithKeyspace("ks1"),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
							NextPages:      20,
						}),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfString("ks1") + // keyspace
					primitives.LengthOfInt*3, // cont. paging options
				nil,
			},
			{
				"query with positional values and keyspace",
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(WithKeyspace("ks1"),
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				primitives.LengthOfLongString("SELECT") +
					primitives.LengthOfShort + // consistency
					primitives.LengthOfInt + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitives.LengthOfInt + // value 2
					primitives.LengthOfInt + // value 3
					primitives.LengthOfString("ks1"), // keyspace
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, cassandraprotocol.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestQueryCodec_Decode(t *testing.T) {
	codec := &QueryCodec{}
	// tests for version 3
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersion3), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with default options",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(),
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
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0b0000_0001, // flags
					0, 2,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
				},
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
						),
					),
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 1, // consistency level
					0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, cassandraprotocol.ProtocolVersion3)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 4
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersion4), func(t *testing.T) {
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
					0, 1, // consistency level
					0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(),
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
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0b0000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 1, // consistency level
					0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, cassandraprotocol.ProtocolVersion4)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(fmt.Sprintf("version %v", cassandraprotocol.ProtocolVersion5), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with keyspace and now-in-seconds",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b0000_0001,    // flags (keyspace)
					0b1000_0000,    // flags (now in seconds)
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 123, // now in seconds
				},
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
				},
				nil,
			},
			{
				"query with positional values, keyspace and now-in-seconds",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123),
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, cassandraprotocol.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(fmt.Sprintf("version %d", cassandraprotocol.ProtocolVersionDse1), func(t *testing.T) {
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
					0, 1, // consistency level
					0, 0, 0, 0, // flags
				},
				&Query{
					Query:   "SELECT",
					Options: NewQueryOptions(),
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
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPageSizeInBytes(),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
						}),
					),
				},
				nil,
			},
			{
				"query with positional values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0, 0, 0, 0b0000_0001, // flags
					0, 3, // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				nil,
			},
			{
				"query with named values",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
					0, 0, 0, 0b0100_0001, // flags
					0, 1, // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Query{
					Query: "SELECT",
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				nil,
			},
			{
				"missing query",
				[]byte{
					0, 0, 0, 0, // empty query
					0, 1, // consistency level
					0, 0, 0, 0, // flags
				},
				nil,
				errors.New("cannot read QUERY empty query string"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, cassandraprotocol.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(fmt.Sprintf("version %v", cassandraprotocol.ProtocolVersionDse2), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"query with keyspace and continuous paging",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(
						WithKeyspace("ks1"),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
							NextPages:      20,
						}),
					),
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
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPageSizeInBytes(),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
						WithContinuousPagingOptions(&ContinuousPagingOptions{
							MaxPages:       50,
							PagesPerSecond: 10,
							NextPages:      20,
						}),
					),
				},
				nil,
			},
			{
				"query with positional values and keyspace",
				[]byte{
					0, 0, 0, 6, S, E, L, E, C, T,
					0, 1, // consistency level
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
					Options: NewQueryOptions(WithKeyspace("ks1"),
						WithPositionalValues(
							&primitives.Value{
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitives.Value{
								Type: primitives.ValueTypeNull,
							},
							&primitives.Value{
								Type: primitives.ValueTypeUnset,
							},
						),
					),
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, cassandraprotocol.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}
