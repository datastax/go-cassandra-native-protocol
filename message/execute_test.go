package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExecuteCodec_Encode(t *testing.T) {
	codec := &ExecuteCodec{}
	// tests for versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
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
							WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
							SkipMetadata(),
							WithPageSize(100),
							WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
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
								&primitive.Value{
									Type:     primitive.ValueTypeRegular,
									Contents: []byte{h, e, l, l, o},
								},
								&primitive.Value{
									Type: primitive.ValueTypeNull,
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
							WithNamedValues(map[string]*primitive.Value{
								"col1": {
									Type:     primitive.ValueTypeRegular,
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
	t.Run(fmt.Sprintf("version %d", primitive.ProtocolVersion4), func(t *testing.T) {
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
						WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
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
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
						WithNamedValues(map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
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
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion4)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(fmt.Sprintf("version %v", primitive.ProtocolVersion5), func(t *testing.T) {
		tests := []encodeTestCase{
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
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(fmt.Sprintf("version %d", primitive.ProtocolVersionDse1), func(t *testing.T) {
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
					0, 0, 0, 0, // flags
				},
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 6, // consistency level
					0, 0, 0, 0b0011_1110, // flags
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
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
							},
						),
					),
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
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
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 1, // consistency level
					0, 0, 0, 0b0100_0001, // flags
					0, 1, // values length
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
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(fmt.Sprintf("version %v", primitive.ProtocolVersionDse2), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"execute with keyspace",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options:          NewQueryOptions(WithKeyspace("ks1")),
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
					0, 1, // consistency level
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b1000_0000,    // flags (keyspace)
					0, 3, k, s, _1, // keyspace
				},
				nil,
			},
			{
				"execute with positional values and keyspace",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: NewQueryOptions(WithKeyspace("ks1"),
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestExecuteCodec_EncodedLength(t *testing.T) {
	codec := &ExecuteCodec{}
	// tests for versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"execute with default options",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(),
					},
					primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte, // flags
					nil,
				},
				{
					"execute with custom options and no values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(
							WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
							SkipMetadata(),
							WithPageSize(100),
							WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
							WithDefaultTimestamp(123),
						),
					},
					primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte + // flags
						primitive.LengthOfInt + // page size
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
						primitive.LengthOfShort + // serial consistency
						primitive.LengthOfLong, // default timestamp
					nil,
				},
				{
					"execute with positional values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(
							WithPositionalValues(
								&primitive.Value{
									Type:     primitive.ValueTypeRegular,
									Contents: []byte{h, e, l, l, o},
								},
								&primitive.Value{
									Type: primitive.ValueTypeNull,
								},
							),
						),
					},
					primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte + // flags
						primitive.LengthOfShort + // values length
						primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
						primitive.LengthOfInt, // value 2
					nil,
				},
				{
					"execute with named values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: NewQueryOptions(
							WithNamedValues(map[string]*primitive.Value{
								"col1": {
									Type:     primitive.ValueTypeRegular,
									Contents: []byte{h, e, l, l, o},
								},
							}),
						),
					},
					primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte + // flags
						primitive.LengthOfShort + // values length
						primitive.LengthOfString("col1") + // name 1
						primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
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
	t.Run(fmt.Sprintf("version %d", primitive.ProtocolVersion4), func(t *testing.T) {
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
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte, // flags
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfInt + // page size
					primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitive.LengthOfShort + // serial consistency
					primitive.LengthOfLong, // default timestamp
				nil,
			},
			{
				"execute with positional values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
							},
						),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt, // value 3
				nil,
			},
			{
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfByte + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfString("col1") + // name 1
					primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
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
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion4)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(fmt.Sprintf("version %v", primitive.ProtocolVersion5), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"execute with keyspace and now-in-seconds",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options:          NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123)),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) + // result metadata id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfString("ks1") + // keyspace
					primitive.LengthOfInt, // now in seconds
				nil,
			},
			{
				"execute with positional values, keyspace and now-in-seconds",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: NewQueryOptions(WithKeyspace("ks1"), WithNowInSeconds(123),
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
							},
						),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) + // result metadata id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt + // value 3
					primitive.LengthOfString("ks1") + // keyspace
					primitive.LengthOfInt, // now in seconds
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
	t.Run(fmt.Sprintf("version %d", primitive.ProtocolVersionDse1), func(t *testing.T) {
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
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt, // flags
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
						WithDefaultTimestamp(123),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfInt + // page size
					primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // paging state
					primitive.LengthOfShort + // serial consistency
					primitive.LengthOfLong, // default timestamp
				nil,
			},
			{
				"execute with positional values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
							},
						),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfBytes([]byte{h, e, l, l, o}) + // value 1
					primitive.LengthOfInt + // value 2
					primitive.LengthOfInt, // value 3
				nil,
			},
			{
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						}),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfShort + // values length
					primitive.LengthOfString("col1") + // name 1
					primitive.LengthOfBytes([]byte{h, e, l, l, o}), // value 1
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
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersionDse1)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v2
	t.Run(fmt.Sprintf("version %v", primitive.ProtocolVersionDse2), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"execute with keyspace",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options:          NewQueryOptions(WithKeyspace("ks1")),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) + // result metadata id
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // flags
					primitive.LengthOfString("ks1"), // keyspace
				nil,
			},
			{
				"execute with positional values and keyspace",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: NewQueryOptions(WithKeyspace("ks1"),
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
							},
						),
					),
				},
				primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) + // query id
					primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) + // result metadata id
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

func TestExecuteCodec_Decode(t *testing.T) {
	codec := &ExecuteCodec{}
	// tests for versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
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
							WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
							SkipMetadata(),
							WithPageSize(100),
							WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
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
								&primitive.Value{
									Type:     primitive.ValueTypeRegular,
									Contents: []byte{h, e, l, l, o},
								},
								&primitive.Value{
									Type: primitive.ValueTypeNull,
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
							WithNamedValues(map[string]*primitive.Value{
								"col1": {
									Type:     primitive.ValueTypeRegular,
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
	t.Run(fmt.Sprintf("version %d", primitive.ProtocolVersion4), func(t *testing.T) {
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
						WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
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
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
						WithNamedValues(map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
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
				actual, err := codec.Decode(source, primitive.ProtocolVersion4)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = 5
	t.Run(fmt.Sprintf("version %v", primitive.ProtocolVersion5), func(t *testing.T) {
		tests := []decodeTestCase{
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
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
				actual, err := codec.Decode(source, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// tests for version = DSE v1
	t.Run(fmt.Sprintf("version %d", primitive.ProtocolVersionDse1), func(t *testing.T) {
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
					0, 0, 0, 0, // flags
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
					0, 0, 0, 0b0011_1110, // flags
					0, 0, 0, 100, // page size
					0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
					0, 9, // serial consistency level
					0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithConsistencyLevel(primitive.ConsistencyLevelLocalQuorum),
						SkipMetadata(),
						WithPageSize(100),
						WithPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
						WithSerialConsistencyLevel(primitive.ConsistencyLevelLocalSerial),
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
					0, 0, 0, 0b0000_0001, // flags
					0, 3, // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
					0, 0, 0, 0b0100_0001, // flags
					0, 1, // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: NewQueryOptions(
						WithNamedValues(map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
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
					0, 0, 0, 0, // flags
				},
				nil,
				errors.New("EXECUTE missing query id"),
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
	t.Run(fmt.Sprintf("version %v", primitive.ProtocolVersionDse2), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"execute with keyspace",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
					0, 1, // consistency level
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b0000_0000,    // flags
					0b1000_0000,    // flags (keyspace)
					0, 3, k, s, _1, // keyspace
					0, 0, 0, 123, // now in seconds
				},
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options:          NewQueryOptions(WithKeyspace("ks1")),
				},
				nil,
			},
			{
				"execute with positional values and keyspace",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
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
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: NewQueryOptions(WithKeyspace("ks1"),
						WithPositionalValues(
							&primitive.Value{
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
							&primitive.Value{
								Type: primitive.ValueTypeNull,
							},
							&primitive.Value{
								Type: primitive.ValueTypeUnset,
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
				actual, err := codec.Decode(source, primitive.ProtocolVersionDse2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}
