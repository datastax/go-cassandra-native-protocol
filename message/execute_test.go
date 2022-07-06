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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestExecute_DeepCopy(t *testing.T) {
	msg := &Execute{
		QueryId:          []byte{0x01},
		ResultMetadataId: []byte{0x02},
		Options: &QueryOptions{
			Consistency: primitive.ConsistencyLevelAll,
			PositionalValues: []*primitive.Value{
				&primitive.Value{
					Type:     primitive.ValueTypeRegular,
					Contents: []byte{0x11},
				},
			},
			NamedValues: map[string]*primitive.Value{
				"1": &primitive.Value{
					Type:     primitive.ValueTypeUnset,
					Contents: []byte{0x21},
				},
			},
			SkipMetadata:      false,
			PageSize:          5,
			PageSizeInBytes:   false,
			PagingState:       []byte{0x33},
			SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
			DefaultTimestamp:  int64Ptr(1),
			Keyspace:          "ks1",
			NowInSeconds:      int32Ptr(3),
			ContinuousPagingOptions: &ContinuousPagingOptions{
				MaxPages:       5,
				PagesPerSecond: 2,
				NextPages:      3,
			},
		},
	}

	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)

	cloned.QueryId = []byte{0x41}
	cloned.ResultMetadataId = []byte{0x52}
	cloned.Options = &QueryOptions{
		Consistency: primitive.ConsistencyLevelLocalOne,
		PositionalValues: []*primitive.Value{
			&primitive.Value{
				Type:     primitive.ValueTypeUnset,
				Contents: []byte{0x21},
			},
		},
		NamedValues: map[string]*primitive.Value{
			"1": &primitive.Value{
				Type:     primitive.ValueTypeNull,
				Contents: []byte{0x31},
			},
		},
		SkipMetadata:      true,
		PageSize:          4,
		PageSizeInBytes:   true,
		PagingState:       []byte{0x23},
		SerialConsistency: nil,
		DefaultTimestamp:  int64Ptr(3),
		Keyspace:          "ks2",
		NowInSeconds:      nil,
		ContinuousPagingOptions: &ContinuousPagingOptions{
			MaxPages:       6,
			PagesPerSecond: 3,
			NextPages:      4,
		},
	}

	assert.Equal(t, []byte{0x01}, msg.QueryId)
	assert.Equal(t, []byte{0x02}, msg.ResultMetadataId)
	assert.Equal(t, primitive.ConsistencyLevelAll, msg.Options.Consistency)
	assert.Equal(t, primitive.ValueTypeRegular, msg.Options.PositionalValues[0].Type)
	assert.Equal(t, []byte{0x11}, msg.Options.PositionalValues[0].Contents)
	assert.Equal(t, primitive.ValueTypeUnset, msg.Options.NamedValues["1"].Type)
	assert.Equal(t, []byte{0x21}, msg.Options.NamedValues["1"].Contents)
	assert.False(t, msg.Options.SkipMetadata)
	assert.EqualValues(t, 5, msg.Options.PageSize)
	assert.False(t, msg.Options.PageSizeInBytes)
	assert.Equal(t, []byte{0x33}, msg.Options.PagingState)
	assert.Equal(t, primitive.ConsistencyLevelLocalSerial, *msg.Options.SerialConsistency)
	assert.EqualValues(t, 1, *msg.Options.DefaultTimestamp)
	assert.Equal(t, "ks1", msg.Options.Keyspace)
	assert.EqualValues(t, 3, *msg.Options.NowInSeconds)
	assert.EqualValues(t, 5, msg.Options.ContinuousPagingOptions.MaxPages)
	assert.EqualValues(t, 2, msg.Options.ContinuousPagingOptions.PagesPerSecond)
	assert.EqualValues(t, 3, msg.Options.ContinuousPagingOptions.NextPages)

	assert.NotEqual(t, msg, cloned)

	assert.Equal(t, []byte{0x41}, cloned.QueryId)
	assert.Equal(t, []byte{0x52}, cloned.ResultMetadataId)
	assert.Equal(t, primitive.ConsistencyLevelLocalOne, cloned.Options.Consistency)
	assert.Equal(t, primitive.ValueTypeUnset, cloned.Options.PositionalValues[0].Type)
	assert.Equal(t, []byte{0x21}, cloned.Options.PositionalValues[0].Contents)
	assert.Equal(t, primitive.ValueTypeNull, cloned.Options.NamedValues["1"].Type)
	assert.Equal(t, []byte{0x31}, cloned.Options.NamedValues["1"].Contents)
	assert.True(t, cloned.Options.SkipMetadata)
	assert.EqualValues(t, 4, cloned.Options.PageSize)
	assert.True(t, cloned.Options.PageSizeInBytes)
	assert.Equal(t, []byte{0x23}, cloned.Options.PagingState)
	assert.Nil(t, cloned.Options.SerialConsistency)
	assert.EqualValues(t, 3, *cloned.Options.DefaultTimestamp)
	assert.Equal(t, "ks2", cloned.Options.Keyspace)
	assert.Nil(t, cloned.Options.NowInSeconds)
	assert.EqualValues(t, 6, cloned.Options.ContinuousPagingOptions.MaxPages)
	assert.EqualValues(t, 3, cloned.Options.ContinuousPagingOptions.PagesPerSecond)
	assert.EqualValues(t, 4, cloned.Options.ContinuousPagingOptions.NextPages)
}

func TestExecuteCodec_Encode(t *testing.T) {
	codec := &executeCodec{}
	// tests for versions < 4
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"execute with default options",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: &QueryOptions{},
					},
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // consistency level
						0, // flags
					},
					nil,
				},
				{
					"execute with custom options and no values",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: &QueryOptions{
							Consistency:       primitive.ConsistencyLevelLocalQuorum,
							SkipMetadata:      true,
							PageSize:          100,
							PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
							SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
							DefaultTimestamp:  int64Ptr(123),
						},
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
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // consistency level
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
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersion4.String(), func(t *testing.T) {
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
					Options: &QueryOptions{},
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
					0, // flags
				},
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
						DefaultTimestamp:  int64Ptr(123),
					},
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
					0, 4, 1, 2, 3, 4, // query id
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
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersion5.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"execute with keyspace and now-in-seconds",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: int32Ptr(123),
					},
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
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
				"execute with positional values, keyspace and now-in-seconds",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: int32Ptr(123),
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
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
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
			{
				"missing query id",
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
	t.Run(primitive.ProtocolVersionDse1.String(), func(t *testing.T) {
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
					Options: &QueryOptions{},
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
					0, 0, 0, 0, // flags
				},
				nil,
			},
			{
				"execute with custom options and no values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
						DefaultTimestamp:  int64Ptr(123),
					},
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
					0, 4, 1, 2, 3, 4, // query id
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
				"execute with named values",
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersionDse2.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"execute with keyspace",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options:          &QueryOptions{Keyspace: "ks1"},
				},
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
					0, 0, // consistency level
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
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
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
			{
				"missing query id",
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
	codec := &executeCodec{}
	// tests for versions < 4
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"execute with default options",
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: &QueryOptions{},
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
						Options: &QueryOptions{
							Consistency:       primitive.ConsistencyLevelLocalQuorum,
							SkipMetadata:      true,
							PageSize:          100,
							PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
							SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
							DefaultTimestamp:  int64Ptr(123),
						},
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
						Options: &QueryOptions{
							NamedValues: map[string]*primitive.Value{
								"col1": {
									Type:     primitive.ValueTypeRegular,
									Contents: []byte{h, e, l, l, o},
								},
							},
						},
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
	t.Run(primitive.ProtocolVersion4.String(), func(t *testing.T) {
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
					Options: &QueryOptions{},
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
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
						DefaultTimestamp:  int64Ptr(123),
					},
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
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
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
	t.Run(primitive.ProtocolVersion5.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"execute with keyspace and now-in-seconds",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: int32Ptr(123),
					},
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
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: int32Ptr(123),
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
	t.Run(primitive.ProtocolVersionDse1.String(), func(t *testing.T) {
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
					Options: &QueryOptions{},
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
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
						DefaultTimestamp:  int64Ptr(123),
					},
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
					Options: &QueryOptions{
						NamedValues: map[string]*primitive.Value{
							"col1": {
								Type:     primitive.ValueTypeRegular,
								Contents: []byte{h, e, l, l, o},
							},
						},
					},
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
	t.Run(primitive.ProtocolVersionDse2.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"execute with keyspace",
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options:          &QueryOptions{Keyspace: "ks1"},
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
	codec := &executeCodec{}
	// tests for versions < 4
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"execute with default options",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // consistency level
						0, // flags
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
						Options: &QueryOptions{},
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
						Options: &QueryOptions{
							Consistency:       primitive.ConsistencyLevelLocalQuorum,
							SkipMetadata:      true,
							PageSize:          100,
							PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
							SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
							DefaultTimestamp:  int64Ptr(123),
						},
					},
					nil,
				},
				{
					"execute with positional values",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // consistency level
						0b0000_0001, // flags
						0, 2,        // values length
						0, 0, 0, 5, h, e, l, l, o, // value 1
						0xff, 0xff, 0xff, 0xff, // value 2
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
					"execute with named values",
					[]byte{
						0, 4, 1, 2, 3, 4, // query id
						0, 0, // consistency level
						0b0100_0001, // flags
						0, 1,        // values length
						0, 4, c, o, l, _1, // name 1
						0, 0, 0, 5, h, e, l, l, o, // value 1
					},
					&Execute{
						QueryId: []byte{1, 2, 3, 4},
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
					"missing query id",
					[]byte{
						0, 0, // query id
						0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersion4.String(), func(t *testing.T) {
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
					0, 0, // consistency level
					0, // flags
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: &QueryOptions{},
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
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
						DefaultTimestamp:  int64Ptr(123),
					},
				},
				nil,
			},
			{
				"execute with positional values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
					0b0000_0001, // flags
					0, 3,        // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"execute with named values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
					0b0100_0001, // flags
					0, 1,        // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"missing query id",
				[]byte{
					0, 0, // query id
					0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersion5.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"execute with keyspace and now-in-seconds",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
					0, 0, // consistency level
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
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: int32Ptr(123),
					},
				},
				nil,
			},
			{
				"execute with positional values, keyspace and now-in-seconds",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
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
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
					Options: &QueryOptions{
						Keyspace:     "ks1",
						NowInSeconds: int32Ptr(123),
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
				"missing result metadata id",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // result metadata id
					0, 0, // consistency level
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
					0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersionDse1.String(), func(t *testing.T) {
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
					0, 0, // consistency level
					0, 0, 0, 0, // flags
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
					Options: &QueryOptions{},
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
					Options: &QueryOptions{
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SkipMetadata:      true,
						PageSize:          100,
						PagingState:       []byte{0xca, 0xfe, 0xba, 0xbe},
						SerialConsistency: consistencyLevelPtr(primitive.ConsistencyLevelLocalSerial),
						DefaultTimestamp:  int64Ptr(123),
					},
				},
				nil,
			},
			{
				"execute with positional values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
					0, 0, 0, 0b0000_0001, // flags
					0, 3, // values length
					0, 0, 0, 5, h, e, l, l, o, // value 1
					0xff, 0xff, 0xff, 0xff, // value 2
					0xff, 0xff, 0xff, 0xfe, // value 3
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"execute with named values",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // consistency level
					0, 0, 0, 0b0100_0001, // flags
					0, 1, // values length
					0, 4, c, o, l, _1, // name 1
					0, 0, 0, 5, h, e, l, l, o, // value 1
				},
				&Execute{
					QueryId: []byte{1, 2, 3, 4},
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
				"missing query id",
				[]byte{
					0, 0, // query id
					0, 0, // consistency level
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
	t.Run(primitive.ProtocolVersionDse2.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"execute with keyspace",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
					0, 0, // consistency level
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
					Options:          &QueryOptions{Keyspace: "ks1"},
				},
				nil,
			},
			{
				"execute with positional values and keyspace",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 4, 5, 6, 7, 8, // result metadata id
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
				&Execute{
					QueryId:          []byte{1, 2, 3, 4},
					ResultMetadataId: []byte{5, 6, 7, 8},
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
			{
				"missing result metadata id",
				[]byte{
					0, 4, 1, 2, 3, 4, // query id
					0, 0, // result metadata id
					0, 0, // consistency level
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
					0, 0, // consistency level
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
