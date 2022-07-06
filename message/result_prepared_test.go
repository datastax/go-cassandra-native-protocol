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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestPreparedResult_DeepCopy(t *testing.T) {
	msg := &PreparedResult{
		PreparedQueryId:  []byte{0x12},
		ResultMetadataId: []byte{0x23},
		VariablesMetadata: &VariablesMetadata{
			PkIndices: []uint16{0},
			Columns: []*ColumnMetadata{
				{
					Keyspace: "ks1",
					Table:    "tb1",
					Name:     "c1",
					Index:    0,
					Type:     datatype.Ascii,
				},
			},
		},
		ResultMetadata: &RowsMetadata{
			ColumnCount:          1,
			PagingState:          nil,
			NewResultMetadataId:  nil,
			ContinuousPageNumber: 1,
			LastContinuousPage:   false,
			Columns: []*ColumnMetadata{
				{
					Keyspace: "ks1",
					Table:    "tb1",
					Name:     "c1",
					Index:    0,
					Type:     datatype.Ascii,
				},
			},
		},
	}

	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)

	cloned.PreparedQueryId = []byte{0x42}
	cloned.ResultMetadataId = []byte{0x51}
	cloned.VariablesMetadata = &VariablesMetadata{
		PkIndices: []uint16{1},
		Columns: []*ColumnMetadata{
			{
				Keyspace: "ks2",
				Table:    "tb2",
				Name:     "c2",
				Index:    0,
				Type:     datatype.Float,
			},
			{
				Keyspace: "ks2",
				Table:    "tb2",
				Name:     "c3",
				Index:    1,
				Type:     datatype.Uuid,
			},
		},
	}
	cloned.ResultMetadata = &RowsMetadata{
		ColumnCount:          1,
		PagingState:          []byte{0x22},
		NewResultMetadataId:  []byte{0x33},
		ContinuousPageNumber: 3,
		LastContinuousPage:   true,
		Columns: []*ColumnMetadata{
			{
				Keyspace: "ks2",
				Table:    "tb2",
				Name:     "c2",
				Index:    0,
				Type:     datatype.Float,
			},
			{
				Keyspace: "ks2",
				Table:    "tb2",
				Name:     "c3",
				Index:    1,
				Type:     datatype.Uuid,
			},
		},
	}

	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, []byte{0x12}, msg.PreparedQueryId)
	assert.Equal(t, []byte{0x23}, msg.ResultMetadataId)
	assert.EqualValues(t, 0, msg.VariablesMetadata.PkIndices[0])
	assert.Equal(t, "ks1", msg.VariablesMetadata.Columns[0].Keyspace)
	assert.Equal(t, "tb1", msg.VariablesMetadata.Columns[0].Table)
	assert.Equal(t, "c1", msg.VariablesMetadata.Columns[0].Name)
	assert.EqualValues(t, 0, msg.VariablesMetadata.Columns[0].Index)
	assert.Equal(t, datatype.Ascii, msg.VariablesMetadata.Columns[0].Type)

	assert.EqualValues(t, 1, msg.ResultMetadata.ColumnCount)
	assert.Nil(t, msg.ResultMetadata.PagingState)
	assert.Nil(t, msg.ResultMetadata.NewResultMetadataId)
	assert.EqualValues(t, 1, msg.ResultMetadata.ContinuousPageNumber)
	assert.False(t, msg.ResultMetadata.LastContinuousPage)
	assert.Equal(t, "ks1", msg.ResultMetadata.Columns[0].Keyspace)
	assert.Equal(t, "tb1", msg.ResultMetadata.Columns[0].Table)
	assert.Equal(t, "c1", msg.ResultMetadata.Columns[0].Name)
	assert.EqualValues(t, 0, msg.ResultMetadata.Columns[0].Index)
	assert.Equal(t, datatype.Ascii, msg.ResultMetadata.Columns[0].Type)

	assert.Equal(t, []byte{0x42}, cloned.PreparedQueryId)
	assert.Equal(t, []byte{0x51}, cloned.ResultMetadataId)
	assert.EqualValues(t, 1, cloned.VariablesMetadata.PkIndices[0])
	assert.Equal(t, "ks2", cloned.VariablesMetadata.Columns[0].Keyspace)
	assert.Equal(t, "tb2", cloned.VariablesMetadata.Columns[0].Table)
	assert.Equal(t, "c2", cloned.VariablesMetadata.Columns[0].Name)
	assert.EqualValues(t, 0, cloned.VariablesMetadata.Columns[0].Index)
	assert.Equal(t, datatype.Float, cloned.VariablesMetadata.Columns[0].Type)
	assert.Equal(t, "ks2", cloned.VariablesMetadata.Columns[1].Keyspace)
	assert.Equal(t, "tb2", cloned.VariablesMetadata.Columns[1].Table)
	assert.Equal(t, "c3", cloned.VariablesMetadata.Columns[1].Name)
	assert.EqualValues(t, 1, cloned.VariablesMetadata.Columns[1].Index)
	assert.Equal(t, datatype.Uuid, cloned.VariablesMetadata.Columns[1].Type)

	assert.EqualValues(t, 1, cloned.ResultMetadata.ColumnCount)
	assert.Equal(t, []byte{0x22}, cloned.ResultMetadata.PagingState)
	assert.Equal(t, []byte{0x33}, cloned.ResultMetadata.NewResultMetadataId)
	assert.EqualValues(t, 3, cloned.ResultMetadata.ContinuousPageNumber)
	assert.True(t, cloned.ResultMetadata.LastContinuousPage)
	assert.Equal(t, "ks2", cloned.ResultMetadata.Columns[0].Keyspace)
	assert.Equal(t, "tb2", cloned.ResultMetadata.Columns[0].Table)
	assert.Equal(t, "c2", cloned.ResultMetadata.Columns[0].Name)
	assert.EqualValues(t, 0, cloned.ResultMetadata.Columns[0].Index)
	assert.Equal(t, datatype.Float, cloned.ResultMetadata.Columns[0].Type)
	assert.Equal(t, "ks2", cloned.ResultMetadata.Columns[1].Keyspace)
	assert.Equal(t, "tb2", cloned.ResultMetadata.Columns[1].Table)
	assert.Equal(t, "c3", cloned.ResultMetadata.Columns[1].Name)
	assert.EqualValues(t, 1, cloned.ResultMetadata.Columns[1].Index)
	assert.Equal(t, datatype.Uuid, cloned.ResultMetadata.Columns[1].Type)
}

func TestResultCodec_Encode_Prepared(test *testing.T) {
	codec := &resultCodec{}
	// versions < 4
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"prepared result without bound variables",
					&PreparedResult{PreparedQueryId: []byte{1, 2, 3, 4}},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
					},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
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
	// versions 4, DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"prepared result without bound variables",
					&PreparedResult{PreparedQueryId: []byte{1, 2, 3, 4}},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
					},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
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
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"prepared result without bound variables",
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
					},
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
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
					},
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
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
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
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepared result without bound variables",
					&PreparedResult{PreparedQueryId: []byte{1, 2, 3, 4}},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables and no result metadata",
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + // col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables and result metadata",
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + // col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort, // col type
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
	// versions 4, DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepared result without bound variables",
					&PreparedResult{PreparedQueryId: []byte{1, 2, 3, 4}},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + // col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + // col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort, // col type
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
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepared result without bound variables",
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and no result metadata",
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + // col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt, // column count
					nil,
				},
				{
					"prepared result with bound variables + partition key indices and result metadata",
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
					primitive.LengthOfInt + // result type
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfShortBytes([]byte{5, 6, 7, 8}) +
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfInt + // pk count
						primitive.LengthOfShort + // pk1
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort + // col type
						primitive.LengthOfInt + // flags
						primitive.LengthOfInt + // column count
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort, // col type
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
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(version.String(), func(test *testing.T) {
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
					&PreparedResult{
						PreparedQueryId:   []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{},
						ResultMetadata:    &RowsMetadata{},
					},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{},
					},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
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
	// versions 4, DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		test.Run(version.String(), func(test *testing.T) {
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
					&PreparedResult{
						PreparedQueryId:   []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{},
						ResultMetadata:    &RowsMetadata{},
					},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{},
					},
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
					&PreparedResult{
						PreparedQueryId: []byte{1, 2, 3, 4},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
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
		test.Run(version.String(), func(test *testing.T) {
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
					&PreparedResult{
						PreparedQueryId:   []byte{1, 2, 3, 4},
						ResultMetadataId:  []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{},
						ResultMetadata:    &RowsMetadata{},
					},
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
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{},
					},
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
					&PreparedResult{
						PreparedQueryId:  []byte{1, 2, 3, 4},
						ResultMetadataId: []byte{5, 6, 7, 8},
						VariablesMetadata: &VariablesMetadata{
							PkIndices: []uint16{0},
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col1",
									Index:    0,
									Type:     datatype.Int,
								},
							},
						},
						ResultMetadata: &RowsMetadata{
							ColumnCount: 1,
							Columns: []*ColumnMetadata{
								{
									Keyspace: "ks1",
									Table:    "table1",
									Name:     "col2",
									Index:    0,
									Type:     datatype.Varchar,
								},
							},
						},
					},
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
