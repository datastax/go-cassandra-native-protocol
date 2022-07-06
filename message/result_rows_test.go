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

func TestRowsResult_DeepCopy(t *testing.T) {
	msg := &RowsResult{
		Metadata: &RowsMetadata{
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
		Data: RowSet{
			{
				{0x12, 0x23},
			},
			{
				{0x44, 0x55},
			},
		},
	}

	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)

	cloned.Metadata = &RowsMetadata{
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
	cloned.Data = RowSet{
		{
			{0x52, 0x63},
		},
	}

	assert.NotEqual(t, msg, cloned)

	assert.EqualValues(t, 1, msg.Metadata.ColumnCount)
	assert.Nil(t, msg.Metadata.PagingState)
	assert.Nil(t, msg.Metadata.NewResultMetadataId)
	assert.EqualValues(t, 1, msg.Metadata.ContinuousPageNumber)
	assert.False(t, msg.Metadata.LastContinuousPage)
	assert.Equal(t, "ks1", msg.Metadata.Columns[0].Keyspace)
	assert.Equal(t, "tb1", msg.Metadata.Columns[0].Table)
	assert.Equal(t, "c1", msg.Metadata.Columns[0].Name)
	assert.EqualValues(t, 0, msg.Metadata.Columns[0].Index)
	assert.Equal(t, datatype.Ascii, msg.Metadata.Columns[0].Type)
	assert.Equal(t, RowSet{{{0x12, 0x23}}, {{0x44, 0x55}}}, msg.Data)

	assert.EqualValues(t, 1, cloned.Metadata.ColumnCount)
	assert.Equal(t, []byte{0x22}, cloned.Metadata.PagingState)
	assert.Equal(t, []byte{0x33}, cloned.Metadata.NewResultMetadataId)
	assert.EqualValues(t, 3, cloned.Metadata.ContinuousPageNumber)
	assert.True(t, cloned.Metadata.LastContinuousPage)
	assert.Equal(t, "ks2", cloned.Metadata.Columns[0].Keyspace)
	assert.Equal(t, "tb2", cloned.Metadata.Columns[0].Table)
	assert.Equal(t, "c2", cloned.Metadata.Columns[0].Name)
	assert.EqualValues(t, 0, cloned.Metadata.Columns[0].Index)
	assert.Equal(t, datatype.Float, cloned.Metadata.Columns[0].Type)
	assert.Equal(t, "ks2", cloned.Metadata.Columns[1].Keyspace)
	assert.Equal(t, "tb2", cloned.Metadata.Columns[1].Table)
	assert.Equal(t, "c3", cloned.Metadata.Columns[1].Name)
	assert.EqualValues(t, 1, cloned.Metadata.Columns[1].Index)
	assert.Equal(t, datatype.Uuid, cloned.Metadata.Columns[1].Type)
	assert.Equal(t, RowSet{{{0x52, 0x63}}}, cloned.Data)
}

func TestResultCodec_Encode_Rows(test *testing.T) {
	row1 := Row{
		Column{0, 0, 0, 1},    // int = 1
		Column{h, e, l, l, o}, // varchar = "hello"
	}
	row2 := Row{
		Column{0, 0, 0, 2},    // int = 2
		Column{w, o, r, l, d}, // varchar = "world"
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
	codec := &resultCodec{}
	// versions < 5
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion5) {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
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
	// version = 5
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:         2,
							Columns:             []*ColumnMetadata{spec1, spec2},
							NewResultMetadataId: []byte{1, 2, 3, 4},
							PagingState:         []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
	// DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse1} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
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
					"rows result with continuous paging",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:          2,
							Columns:              []*ColumnMetadata{spec1, spec2},
							LastContinuousPage:   true,
							ContinuousPageNumber: 42,
						},
						Data: RowSet{row1, row2},
					},
					[]byte{
						0, 0, 0, 2, // result type
						0b1100_0000, 0, 0, 1, // flags (last page | page no | global table spec)
						0, 0, 0, 2, // column count
						0, 0, 0, 42, // continuous paging number
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
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
	// DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse2} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							NewResultMetadataId: []byte{1, 2, 3, 4},
							ColumnCount:         2,
							Columns:             []*ColumnMetadata{spec1, spec2},
							PagingState:         []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
				{
					"rows result with continuous paging",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:          2,
							Columns:              []*ColumnMetadata{spec1, spec2},
							LastContinuousPage:   true,
							ContinuousPageNumber: 42,
						},
						Data: RowSet{row1, row2},
					},
					[]byte{
						0, 0, 0, 2, // result type
						0b1100_0000, 0, 0, 1, // flags (last page | page no | global table spec)
						0, 0, 0, 2, // column count
						0, 0, 0, 42, // continuous paging number
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
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

func TestResultCodec_EncodedLength_Rows(test *testing.T) {
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
	codec := &resultCodec{}
	// versions < 5
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion5) {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table2") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
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
	// version = 5
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table2") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata and new result metadata id",
					&RowsResult{
						Metadata: &RowsMetadata{
							NewResultMetadataId: []byte{1, 2, 3, 4},
							ColumnCount:         2,
							Columns:             []*ColumnMetadata{spec1, spec2},
							PagingState:         []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
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
	// DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse1} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table2") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with continuous paging",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:          2,
							Columns:              []*ColumnMetadata{spec1, spec2},
							LastContinuousPage:   true,
							ContinuousPageNumber: 42,
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
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
	// DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse2} {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"rows result without column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata no global table spec last page",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table2") +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with column metadata and new result metadata id",
					&RowsResult{
						Metadata: &RowsMetadata{
							NewResultMetadataId: []byte{1, 2, 3, 4},
							ColumnCount:         2,
							Columns:             []*ColumnMetadata{spec1, spec2},
							PagingState:         []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) +
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
					nil,
				},
				{
					"rows result with continuous paging",
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:          2,
							Columns:              []*ColumnMetadata{spec1, spec2},
							LastContinuousPage:   true,
							ContinuousPageNumber: 42,
						},
						Data: RowSet{row1, row2},
					},
					primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfInt +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1") +
						primitive.LengthOfString("col1") +
						primitive.LengthOfShort +
						primitive.LengthOfString("col2") +
						primitive.LengthOfShort +
						primitive.LengthOfInt +
						8*2 + 9*2, // data
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
	codec := &resultCodec{}
	// versions < 5
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion5) {
		test.Run(version.String(), func(test *testing.T) {
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
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
	// versions = 5
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5} {
		test.Run(version.String(), func(test *testing.T) {
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
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
	// DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse1} {
		test.Run(version.String(), func(test *testing.T) {
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
					nil,
				},
				{
					"rows result with continuous paging",
					[]byte{
						0, 0, 0, 2, // result type
						0b1100_0000, 0, 0, 1, // flags (last page | page no | global table spec)
						0, 0, 0, 2, // column count
						0, 0, 0, 42, // continuous paging number
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:          2,
							Columns:              []*ColumnMetadata{spec1, spec2},
							LastContinuousPage:   true,
							ContinuousPageNumber: 42,
						},
						Data: RowSet{row1, row2},
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
	// DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse2} {
		test.Run(version.String(), func(test *testing.T) {
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec2},
							PagingState: []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount: 2,
							Columns:     []*ColumnMetadata{spec1, spec3},
						},
						Data: RowSet{row1, row2},
					},
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
					&RowsResult{
						Metadata: &RowsMetadata{
							NewResultMetadataId: []byte{1, 2, 3, 4},
							ColumnCount:         2,
							Columns:             []*ColumnMetadata{spec1, spec2},
							PagingState:         []byte{0xca, 0xfe, 0xba, 0xbe},
						},
						Data: RowSet{row1, row2},
					},
					nil,
				},
				{
					"rows result with continuous paging",
					[]byte{
						0, 0, 0, 2, // result type
						0b1100_0000, 0, 0, 1, // flags (last page | page no | global table spec)
						0, 0, 0, 2, // column count
						0, 0, 0, 42, // continuous paging number
						0, 3, k, s, _1, // col1 ks
						0, 6, t, a, b, l, e, _1, // col1 table
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
					&RowsResult{
						Metadata: &RowsMetadata{
							ColumnCount:          2,
							Columns:              []*ColumnMetadata{spec1, spec2},
							LastContinuousPage:   true,
							ContinuousPageNumber: 42,
						},
						Data: RowSet{row1, row2},
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
