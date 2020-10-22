package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"testing"
)

func TestExecuteCodec_Encode(t *testing.T) {
	codec := &ExecuteCodec{}
	// tests for versions <= 3
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersion3; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected []byte
				err      error
			}{
				{
					"execute with default options",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(),
					},
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 1, // consistency level
						0, // flags
					},
					nil,
				},
				{
					"execute with custom options and no values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
						0, 4, 1, 2, 3, 4, // query id
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
					"execute with positional values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
						0, 4, 1, 2, 3, 4, // query id
						0, 1, // consistency level
						0b0000_0001, // flags
						0, 2,        // values length
						0, 0, 0, 5, h, e, l, l, o, // value 1
						0xff, 0xff, 0xff, 0xff, // value 2
					},
					nil,
				},
				{
					"execute with named values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
						0, 4, 1, 2, 3, 4, // query id
						0, 1, // consistency level
						0b0100_0001, // flags
						0, 1,        // values length
						0, 4, c, o, l, _1, // name 1
						0, 0, 0, 5, h, e, l, l, o, // value 1
					},
					nil,
				},
				{
					"missing query id",
					&Execute{},
					nil,
					errors.New("EXECUTE missing query id"),
				},
				{
					"not an execute",
					&Options{},
					nil,
					errors.New("expected *message.Execute, got *message.Options"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// tests for version = 4
	t.Run("version 4", func(t *testing.T) {
		tests := []struct {
			name     string
			input    Message
			expected []byte
			err      error
		}{
			{
				"execute with default options",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(),
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 1, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
					0, 4, 1, 2, 3, 4, // query id
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
				"execute with positional values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
					0, 4, 1, 2, 3, 4, // query id
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
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
					0, 4, 1, 2, 3, 4, // query id
					0, 1, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				nil,
			},
			{
				"missing query id",
				&Execute{},
				nil,
				errors.New("EXECUTE missing query id"),
			},
			{
				"not an execute",
				&Options{},
				nil,
				errors.New("expected *message.Execute, got *message.Options"),
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
	// tests for version >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected []byte
				err      error
			}{
				{
					"execute with keyspace and now-in-seconds",
					&Execute{
						QueryId:          []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						Options:          NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
					},
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 4, 5, 6, 7, 8, // result metadata id
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
					"execute with positional values, keyspace and now-in-seconds",
					&Execute{
						QueryId:          []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
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
						0, 4, 1, 2, 3, 4, // query id
						0, 4, 5, 6, 7, 8, // result metadata id
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
				{
					"missing result metadata id",
					&Execute{},
					nil,
					errors.New("EXECUTE missing query id"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestExecuteCodec_EncodedLength(t *testing.T) {
	codec := &ExecuteCodec{}
	// tests for versions <= 3
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersion3; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected int
				err      error
			}{
				{
					"execute with default options",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(),
					},
					primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte, // flags
					nil,
				},
				{
					"execute with custom options and no values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(
							WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
							SkipMetadata(),
							WithPageSize(100),
							WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
							WithDefaultTimestamp(123),
						),
					},
					primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte + // flags
						primitives.LengthOfInt + // page size
						primitives.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
						primitives.LengthOfShort + // serial consistency
						primitives.LengthOfLong, // default timestamp
					nil,
				},
				{
					"execute with positional values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
					primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte + // flags
						primitives.LengthOfShort + // values length
						primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
						primitives.LengthOfInt, // value 2
					nil,
				},
				{
					"execute with named values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(
							WithNamedValues(map[string]*primitives.Value{
								"col1": {
									Type:     primitives.ValueTypeRegular,
									Contents: []byte{h, e, l, l, o},
								},
							}),
						),
					},
					primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte + // flags
						primitives.LengthOfShort + // values length
						primitives.LengthOfString("col1") + // name 1
						primitives.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
					nil,
				},
				{
					"not an execute",
					&Options{},
					-1,
					errors.New("expected *message.Execute, got *message.Options"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// tests for version = 4
	t.Run("version 4", func(t *testing.T) {
		tests := []struct {
			name     string
			input    Message
			expected int
			err      error
		}{
			{
				"execute with default options",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(),
				},
				primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte, // flags
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfInt + // page size
					primitives.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitives.LengthOfShort + // serial consistency
					primitives.LengthOfLong, // default timestamp
				nil,
			},
			{
				"execute with positional values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitives.LengthOfInt + // value 2
					primitives.LengthOfInt, // value 3
				nil,
			},
			{
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitives.Value{
							"col1": {
								Type:     primitives.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitives.LengthOfShort + // consistency
					primitives.LengthOfByte + // flags
					primitives.LengthOfShort + // values length
					primitives.LengthOfString("col1") + // name 1
					primitives.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
				nil,
			},
			{
				"not an execute",
				&Options{},
				-1,
				errors.New("expected *message.Execute, got *message.Options"),
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
	// tests for version >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected int
				err      error
			}{
				{
					"execute with keyspace and now-in-seconds",
					&Execute{
						QueryId:          []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						Options:          NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
					},
					primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitives.LengthOfShortBytes([]byte{5, 6, 7, 8}) + // result metadata id
						primitives.LengthOfShort + // consistency
						primitives.LengthOfInt + // flags
						primitives.LengthOfString("ks1") + // keyspace
						primitives.LengthOfInt, // now in seconds
					nil,
				},
				{
					"execute with positional values, keyspace and now-in-seconds",
					&Execute{
						QueryId:          []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
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
					primitives.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitives.LengthOfShortBytes([]byte{5, 6, 7, 8}) + // result metadata id
						primitives.LengthOfShort + // consistency
						primitives.LengthOfInt + // flags
						primitives.LengthOfShort + // values length
						primitives.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
						primitives.LengthOfInt + // value 2
						primitives.LengthOfInt + // value 3
						primitives.LengthOfString("ks1") + // keyspace
						primitives.LengthOfInt, // now in seconds
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestExecuteCodec_Decode(t *testing.T) {
	codec := &ExecuteCodec{}
	// tests for versions <= 3
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersion3; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected Message
				err      error
			}{
				{
					"execute with default options",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 1, // consistency level
						0, // flags
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(),
					},
					nil,
				},
				{
					"execute with custom options and no values",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 6, // consistency level
						0b0011_1110,  // flags
						0, 0, 0, 100, // page size
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 9, // serial consistency level
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
					"execute with positional values",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 1, // consistency level
						0b0000_0001, // flags
						0, 2,        // values length
						0, 0, 0, 5, h, e, l, l, o, // value 1
						0xff, 0xff, 0xff, 0xff, // value 2
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
					"execute with named values",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 1, // consistency level
						0b0100_0001, // flags
						0, 1,        // values length
						0, 4, c, o, l, _1, // name 1
						0, 0, 0, 5, h, e, l, l, o, // value 1
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
					"missing query id",
					[]byte{
						0, 0, // query id
						0, 1, // consistency level
						0, // flags
					},
					nil,
					errors.New("EXECUTE missing query id"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// tests for version = 4
	t.Run("version 4", func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected Message
			err      error
		}{
			{
				"execute with default options",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 1, // consistency level
					0, // flags
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(),
				},
				nil,
			},
			{
				"execute with custom options and no values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 6, // consistency level
					0b0011_1110,  // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"execute with positional values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 1, // consistency level
					0b0000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"execute with named values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 1, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"missing query id",
				[]byte{
					0, 0, // query id
					0, 1, // consistency level
					0, // flags
				},
				nil,
				errors.New("EXECUTE missing query id"),
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
	// tests for version >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected Message
				err      error
			}{
				{
					"execute with keyspace and now-in-seconds",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 4, 5, 6, 7, 8, // result metadata id
						0, 1, // consistency level
						0b0000_0000,    // flags
						0b0000_0000,    // flags
						0b0000_0001,    // flags (keyspace)
						0b1000_0000,    // flags (now in seconds)
						0, 3, k, s, _1, // keyspace
						0, 0, 0, 123, // now in seconds
					},
					&Execute{
						QueryId:          []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						Options:          NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
					},
					nil,
				},
				{
					"execute with positional values, keyspace and now-in-seconds",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 4, 5, 6, 7, 8, // result metadata id
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
					&Execute{
						QueryId:          []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
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
				{
					"missing result metadata id",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // result metadata id
						0, 1, // consistency level
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b0000_0000, // flags
					},
					nil,
					errors.New("EXECUTE missing result metadata id"),
				},
				{
					"missing query id",
					[]byte{
						0, 0, // query id
						0, 4, 1, 2, 3, 4, // result metadata id
						0, 1, // consistency level
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b0000_0000, // flags
					},
					nil,
					errors.New("EXECUTE missing query id"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}
