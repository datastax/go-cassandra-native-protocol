package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchCodec_Encode(t *testing.T) {
	codec := &batchCodec{}
	// versions <= 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"not a batch",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					nil,
					errors.New("expected *message.Batch, got *message.AuthChallenge"),
				},
				{
					"invalid batch type",
					&Batch{Type: primitive.BatchType(42)},
					nil,
					errors.New("invalid BATCH type: BatchType ? [0X2A]"),
				},
				{
					"empty batch",
					&Batch{},
					[]byte{byte(primitive.BatchTypeLogged)},
					errors.New("BATCH messages must contain at least one child query"),
				},
				{
					"batch with 2 children",
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 0, // consistency level
						0, // flags
					},
					nil,
				},
				{
					"batch with custom options",
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
					[]byte{
						byte(primitive.BatchTypeUnlogged),
						0, 1, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						0, 6, // consistency
						0b0011_0000, // flags
						0, 9,        // serial consistency
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					},
					nil,
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
	// versions = DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse1} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"not a batch",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					nil,
					errors.New("expected *message.Batch, got *message.AuthChallenge"),
				},
				{
					"invalid batch type",
					&Batch{Type: primitive.BatchType(42)},
					nil,
					errors.New("invalid BATCH type: BatchType ? [0X2A]"),
				},
				{
					"empty batch",
					&Batch{},
					[]byte{byte(primitive.BatchTypeLogged)},
					errors.New("BATCH messages must contain at least one child query"),
				},
				{
					"batch with 2 children",
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					nil,
				},
				{
					"batch with custom options",
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
					[]byte{
						byte(primitive.BatchTypeUnlogged),
						0, 1, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						0, 6, // consistency
						0, 0, 0, 0b0011_0000, // flags
						0, 9, // serial consistency
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					},
					nil,
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
	// versions 5, DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse2} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"not a batch",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					nil,
					errors.New("expected *message.Batch, got *message.AuthChallenge"),
				},
				{
					"invalid batch type",
					&Batch{Type: primitive.BatchType(42)},
					nil,
					errors.New("invalid BATCH type: BatchType ? [0X2A]"),
				},
				{
					"empty batch",
					&Batch{},
					[]byte{byte(primitive.BatchTypeLogged)},
					errors.New("BATCH messages must contain at least one child query"),
				},
				{
					"batch with 2 children",
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					nil,
				},
				{
					"batch with custom options",
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
						Keyspace:          "ks1",
					},
					[]byte{
						byte(primitive.BatchTypeUnlogged),
						0, 1, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						0, 6, // consistency
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b1011_0000, // flags => 0x10 | 0x20 | 0x80
						0, 9,        // serial consistency
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
						0, 3, k, s, _1, // keyspace
					},
					nil,
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

func TestBatchCodec_EncodedLength(t *testing.T) {
	codec := &batchCodec{}
	// versions <= 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"not a batch",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					-1,
					errors.New("expected *message.Batch, got *message.AuthChallenge"),
				},
				{
					"empty batch",
					&Batch{},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte, // flags
					nil,
				},
				{
					"batch with 2 children",
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfByte + // child 1 kind
						primitive.LengthOfLongString("INSERT") + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitive.LengthOfByte + // child 2 kind
						primitive.LengthOfShortBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 2 value 1
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte, // flags
					nil,
				},
				{
					"batch with custom options",
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfByte + // child 1 kind
						primitive.LengthOfLongString("INSERT") + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitive.LengthOfShort + // consistency
						primitive.LengthOfByte + // flags
						primitive.LengthOfShort + // serial consistency
						primitive.LengthOfLong, // default timestamp
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
	// versions = DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse1} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"not a batch",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					-1,
					errors.New("expected *message.Batch, got *message.AuthChallenge"),
				},
				{
					"empty batch",
					&Batch{},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt, // flags
					nil,
				},
				{
					"batch with 2 children",
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfByte + // child 1 kind
						primitive.LengthOfLongString("INSERT") + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitive.LengthOfByte + // child 2 kind
						primitive.LengthOfShortBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 2 value 1
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt, // flags
					nil,
				},
				{
					"batch with custom options",
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfByte + // child 1 kind
						primitive.LengthOfLongString("INSERT") + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // flags
						primitive.LengthOfShort + // serial consistency
						primitive.LengthOfLong, // default timestamp
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
	// versions 5, DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse2} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"not a batch",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					-1,
					errors.New("expected *message.Batch, got *message.AuthChallenge"),
				},
				{
					"empty batch",
					&Batch{},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt, // flags
					nil,
				},
				{
					"batch with 2 children",
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfByte + // child 1 kind
						primitive.LengthOfLongString("INSERT") + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitive.LengthOfByte + // child 2 kind
						primitive.LengthOfShortBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 2 value 1
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt, // flags
					nil,
				},
				{
					"batch with custom options",
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
						Keyspace:          "ks1",
						NowInSeconds:      &primitive.NillableInt32{Value: 234},
					},
					primitive.LengthOfByte +
						primitive.LengthOfShort + // children count
						primitive.LengthOfByte + // child 1 kind
						primitive.LengthOfLongString("INSERT") + // child 1 query
						primitive.LengthOfShort + // child values count
						primitive.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // flags
						primitive.LengthOfShort + // serial consistency
						primitive.LengthOfLong + // default timestamp
						primitive.LengthOfString("ks1") + // keyspace
						primitive.LengthOfInt, // now in seconds
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

func TestBatchCodec_Decode(t *testing.T) {
	codec := &batchCodec{}
	// versions <= 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"invalid batch type",
					[]byte{
						42,   // bach type
						0, 0, // children count
						0, 0, // consistency level
						0, // flags
					},
					nil,
					errors.New("invalid BATCH type: BatchType ? [0X2A]"),
				},
				{
					"empty batch",
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 0, // children count
						0, 0, // consistency level
						0, // flags
					},
					&Batch{Children: []*BatchChild{}},
					nil,
				},
				{
					"batch with 2 children",
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 0, // consistency level
						0, // flags
					},
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					nil,
				},
				{
					"batch with custom options",
					[]byte{
						byte(primitive.BatchTypeUnlogged),
						0, 1, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						0, 6, // consistency
						0b0011_0000, // flags
						0, 9,        // serial consistency
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					},
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
					nil,
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
	// versions = DSE v1
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersionDse1} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"invalid batch type",
					[]byte{
						42,   // bach type
						0, 0, // children count
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					nil,
					errors.New("invalid BATCH type: BatchType ? [0X2A]"),
				},
				{
					"empty batch",
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 0, // children count
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					&Batch{Children: []*BatchChild{}},
					nil,
				},
				{
					"batch with 2 children",
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					nil,
				},
				{
					"batch with custom options",
					[]byte{
						byte(primitive.BatchTypeUnlogged),
						0, 1, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						0, 6, // consistency
						0, 0, 0, 0b0011_0000, // flags
						0, 9, // serial consistency
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
					},
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
					},
					nil,
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
	// versions 5, DSE v2
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse2} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"invalid batch type",
					[]byte{
						42,   // bach type
						0, 0, // children count
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					nil,
					errors.New("invalid BATCH type: BatchType ? [0X2A]"),
				},
				{
					"empty batch",
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 0, // children count
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					&Batch{Children: []*BatchChild{}},
					nil,
				},
				{
					"batch with 2 children",
					[]byte{
						byte(primitive.BatchTypeLogged),
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 0, // consistency level
						0, 0, 0, 0, // flags
					},
					&Batch{
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
							{
								QueryOrId: []byte{0xca, 0xfe, 0xba, 0xbe},
								Values:    []*primitive.Value{primitive.NewValue([]byte{5, 6, 7, 8})},
							},
						},
					},
					nil,
				},
				{
					"batch with custom options",
					[]byte{
						byte(primitive.BatchTypeUnlogged),
						0, 1, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						0, 6, // consistency
						0b0000_0000, // flags
						0b0000_0000, // flags
						0b0000_0001, // flags => 0x100
						0b1011_0000, // flags => 0x10 | 0x20 | 0x80
						0, 9,        // serial consistency
						0, 0, 0, 0, 0, 0, 0, 123, // default timestamp
						0, 3, k, s, _1, // keyspace
						0, 0, 0, 234, // now in seconds
					},
					&Batch{
						Type: primitive.BatchTypeUnlogged,
						Children: []*BatchChild{
							{
								QueryOrId: "INSERT",
								Values:    []*primitive.Value{primitive.NewValue([]byte{1, 2, 3, 4})},
							},
						},
						Consistency:       primitive.ConsistencyLevelLocalQuorum,
						SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
						DefaultTimestamp:  &primitive.NillableInt64{Value: 123},
						Keyspace:          "ks1",
						NowInSeconds:      &primitive.NillableInt32{Value: 234},
					},
					nil,
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
