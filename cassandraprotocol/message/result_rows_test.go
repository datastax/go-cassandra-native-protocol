package message

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/datatype"
	"testing"
)

func TestResultCodec_Encode_Rows(test *testing.T) {
	row1 := [][]byte{
		{0, 0, 0, 1},    // int = 1
		{h, e, l, l, o}, // varchar = "hello"
	}
	row2 := [][]byte{
		{0, 0, 0, 2},    // int = 2
		{w, o, r, l, d}, // varchar = "world"
	}
	spec1 := &ColumnMetadata{
		Keyspace: "ks1",
		Table:    "table1",
		Name:     "col1",
		Index:    0,
		Type:     datatype.Int,
	}
	spec2 := &ColumnMetadata{
		Keyspace: "ks1",
		Table:    "table1",
		Name:     "col2",
		Index:    0,
		Type:     datatype.Varchar,
	}
	spec3 := &ColumnMetadata{
		Keyspace: "ks1",
		Table:    "table2",
		Name:     "col2",
		Index:    0,
		Type:     datatype.Varchar,
	}
	codec := &ResultCodec{}
	// versions < 5
	for version := cassandraprotocol.ProtocolVersionMin; version < cassandraprotocol.ProtocolVersion5; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"rows result without column metadata",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							NoColumnMetadata(2))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 6, // flags (HAS_MORE_PAGES | NO_METADATA)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					nil,
				},
				{
					"rows result with column metadata",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithColumns(spec1, spec2))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 3, // flags (GLOBAL_TABLE_SPEC | HAS_MORE_PAGES)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithColumns(spec1, spec3))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 0, // flags
						0, 0, 0, 2, // column count
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 3, k, s, _1, // col2 ks
						0, 6, t, a, b, l, e, _2, // col2 table
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
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
	// versions >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"rows result without column metadata",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							NoColumnMetadata(2))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 6, // flags (HAS_MORE_PAGES | NO_METADATA)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					nil,
				},
				{
					"rows result with column metadata",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithColumns(spec1, spec2))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 3, // flags (GLOBAL_TABLE_SPEC | HAS_MORE_PAGES)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithColumns(spec1, spec3))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 0, // flags
						0, 0, 0, 2, // column count
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 3, k, s, _1, // col2 ks
						0, 6, t, a, b, l, e, _2, // col2 table
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					nil,
				},
				{
					"rows result with column metadata and new result metadata id",
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithNewResultMetadataId([]byte{1, 2, 3, 4}),
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithColumns(spec1, spec2))),
						WithRowsData(row1, row2),
					),
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 11, // flags (GLOBAL_TABLE_SPEC | HAS_MORE_PAGES | METADATA_CHANGED)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 4, 1, 2, 3, 4, // new result metadata id
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
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

func TestResultCodec_Decode_Rows(test *testing.T) {
	row1 := [][]byte{
		{0, 0, 0, 1},    // int = 1
		{h, e, l, l, o}, // varchar = "hello"
	}
	row2 := [][]byte{
		{0, 0, 0, 2},    // int = 2
		{w, o, r, l, d}, // varchar = "world"
	}
	spec1 := &ColumnMetadata{
		Keyspace: "ks1",
		Table:    "table1",
		Name:     "col1",
		Index:    0,
		Type:     datatype.Int,
	}
	spec2 := &ColumnMetadata{
		Keyspace: "ks1",
		Table:    "table1",
		Name:     "col2",
		Index:    0,
		Type:     datatype.Varchar,
	}
	spec3 := &ColumnMetadata{
		Keyspace: "ks1",
		Table:    "table2",
		Name:     "col2",
		Index:    0,
		Type:     datatype.Varchar,
	}
	codec := &ResultCodec{}
	// versions < 5
	for version := cassandraprotocol.ProtocolVersionMin; version < cassandraprotocol.ProtocolVersion5; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"rows result without column metadata",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 6, // flags (HAS_MORE_PAGES | NO_METADATA)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							NoColumnMetadata(2))),
						WithRowsData(row1, row2),
					),
					nil,
				},
				{
					"rows result with column metadata",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 3, // flags (GLOBAL_TABLE_SPEC | HAS_MORE_PAGES)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithColumns(spec1, spec2))),
						WithRowsData(row1, row2),
					),
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 0, // flags
						0, 0, 0, 2, // column count
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 3, k, s, _1, // col2 ks
						0, 6, t, a, b, l, e, _2, // col2 table
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithColumns(spec1, spec3))),
						WithRowsData(row1, row2),
					),
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
	// versions >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"rows result without column metadata",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 6, // flags (HAS_MORE_PAGES | NO_METADATA)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							NoColumnMetadata(2))),
						WithRowsData(row1, row2),
					),
					nil,
				},
				{
					"rows result with column metadata",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 3, // flags (GLOBAL_TABLE_SPEC | HAS_MORE_PAGES)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithColumns(spec1, spec2))),
						WithRowsData(row1, row2),
					),
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 0, // flags
						0, 0, 0, 2, // column count
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 3, k, s, _1, // col2 ks
						0, 6, t, a, b, l, e, _2, // col2 table
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithColumns(spec1, spec3))),
						WithRowsData(row1, row2),
					),
					nil,
				},
				{
					"rows result with column metadata and new result metadata id",
					[]byte{
						0, 0, 0, 2, // result type
						0, 0, 0, 11, // flags (GLOBAL_TABLE_SPEC | HAS_MORE_PAGES | METADATA_CHANGED)
						0, 0, 0, 2, // column count
						0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe, // paging state
						0, 4, 1, 2, 3, 4, // new result metadata id
						0, 3, k, s, _1, // global ks
						0, 6, t, a, b, l, e, _1, // global table
						0, 4, c, o, l, _1, // col1 name
						0, 9, // col1 type
						0, 4, c, o, l, _2, // col2 name
						0, 13, // col2 type
						0, 0, 0, 2, // rows count
						0, 0, 0, 4, 0, 0, 0, 1, // row1, col1
						0, 0, 0, 5, h, e, l, l, o, // row1, col2
						0, 0, 0, 4, 0, 0, 0, 2, // row2, col1
						0, 0, 0, 5, w, o, r, l, d, // row2, col2
					},
					NewRowsResult(
						WithRowsMetadata(NewRowsMetadata(
							WithNewResultMetadataId([]byte{1, 2, 3, 4}),
							WithResultPagingState([]byte{0xca, 0xfe, 0xba, 0xbe}),
							WithColumns(spec1, spec2))),
						WithRowsData(row1, row2),
					),
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
