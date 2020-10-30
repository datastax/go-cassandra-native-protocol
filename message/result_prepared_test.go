package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResultCodec_Encode_Prepared(test *testing.T) {
	codec := &resultCodec{}
	// versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"prepared result without bound variables",
					NewPreparedResult(WithPreparedQueryId([]byte{1, 2, 3, 4})),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 0, // flags
						0, 0, 0, 0, // column count
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					nil,
				},
				{
					"prepared result with bound variables and no result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}))),
					),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					nil,
				},
				{
					"prepared result with bound variables and result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}))),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})))),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _2, // col1 name
						0, 13, // col1 type
					},
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// version 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		test.Run(fmt.Sprintf("version %d", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"prepared result without bound variables",
					NewPreparedResult(WithPreparedQueryId([]byte{1, 2, 3, 4})),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 0, // flags
						0, 0, 0, 0, // column count
						0, 0, 0, 0, // pk count
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
					),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})),
						)),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _2, // col1 name
						0, 13, // col1 type
					},
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// versions 5, DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse2} {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"prepared result without bound variables",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
					),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						0, 4, 5, 6, 7, 8, // result metadata id
						// variables metadata
						0, 0, 0, 0, // flags
						0, 0, 0, 0, // column count
						0, 0, 0, 0, // pk count
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
					),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						0, 4, 5, 6, 7, 8, // result metadata id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})),
						)),
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						0, 4, 5, 6, 7, 8, // result metadata id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _2, // col1 name
						0, 13, // col1 type
					},
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestResultCodec_EncodedLength_Prepared(test *testing.T) {
	codec := &resultCodec{}
	// versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepared result without bound variables",
					NewPreparedResult(WithPreparedQueryId([]byte{1, 2, 3, 4})),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + //column count
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables and no result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}))),
					),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + //col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables and result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}))),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})))),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + //col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort, //col type
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// version 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		test.Run(fmt.Sprintf("version %d", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepared result without bound variables",
					NewPreparedResult(WithPreparedQueryId([]byte{1, 2, 3, 4})),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + //column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
					),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + //col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})),
						)),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + //col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort, //col type
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// versions 5, DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse2} {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepared result without bound variables",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
					),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + //column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
					),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + //column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + //col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})),
						)),
					primitive.LengthOfInt + //result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + //column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + //col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort, //col type
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestResultCodec_Decode_Prepared(test *testing.T) {
	codec := &resultCodec{}
	// versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"prepared result without bound variables",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 0, // flags
						0, 0, 0, 0, // column count
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
					),
					nil,
				},
				{
					"prepared result with bound variables and no result metadata",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}))),
					),
					nil,
				},
				{
					"prepared result with bound variables and result metadata",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _2, // col1 name
						0, 13, // col1 type
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}))),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})))),
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// version 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		test.Run(fmt.Sprintf("version %d", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"prepared result without bound variables",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 0, // flags
						0, 0, 0, 0, // column count
						0, 0, 0, 0, // pk count
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
					),
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
					),
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _2, // col1 name
						0, 13, // col1 type
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})),
						)),
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	// versions 5, DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse2} {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"prepared result without bound variables",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						0, 4, 5, 6, 7, 8, // result metadata id
						// variables metadata
						0, 0, 0, 0, // flags
						0, 0, 0, 0, // column count
						0, 0, 0, 0, // pk count
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
					),
					nil,
				},
				{
					"prepared result with bound variables partition key indices and no result metadata",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						0, 4, 5, 6, 7, 8, // result metadata id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 4, // flags (NO_METADATA)
						0, 0, 0, 0, // column count
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
					),
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					[]byte{
						0, 0, 0, 4, // result type
						0, 4, 1, 2, 3, 4, // prepared id
						0, 4, 5, 6, 7, 8, // result metadata id
						// variables metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 0, 0, 1, // pk count
						0, 0, // pk1
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						// result metadata
						0, 0, 0, 1, // flags (GLOBAL_TABLE_SPEC)
						0, 0, 0, 1, // column count
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _2, // col1 name
						0, 13, // col1 type
					},
					NewPreparedResult(
						WithPreparedQueryId([]byte{1, 2, 3, 4}),
						WithResultMetadataId([]byte{5, 6, 7, 8}),
						WithVariablesMetadata(
							NewVariablesMetadata(
								WithPartitionKeyIndices(0),
								WithResultColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								}),
							)),
						WithPreparedResultMetadata(
							NewRowsMetadata(
								WithColumns(&ColumnMetadata{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								})),
						)),
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}
