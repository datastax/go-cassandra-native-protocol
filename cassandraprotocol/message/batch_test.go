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

func TestBatchCodec_Encode(t *testing.T) {
	codec := &BatchCodec{}
	// versions <= 4
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersion4; version++ {
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
					&Batch{Type: cassandraprotocol.BatchType(42)},
					nil,
					errors.New("invalid BATCH type: 42"),
				},
				{
					"empty batch",
					NewBatch(),
					[]byte{cassandraprotocol.BatchTypeLogged},
					errors.New("BATCH messages must contain at least one child query"),
				},
				{
					"batch with 2 children",
					NewBatch(WithBatchChildren(
						NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4})),
						NewPreparedBatchChild([]byte{0xca, 0xfe, 0xba, 0xbe}, primitives.NewValue([]byte{5, 6, 7, 8})),
					)),
					[]byte{
						cassandraprotocol.BatchTypeLogged,
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 1, // consistency
						0, // flags
					},
					nil,
				},
				{
					"batch with custom options",
					NewBatch(
						WithBatchType(cassandraprotocol.BatchTypeUnlogged),
						WithBatchChildren(NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4}))),
						WithBatchConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						WithBatchSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithBatchDefaultTimestamp(123),
					),
					[]byte{
						cassandraprotocol.BatchTypeUnlogged,
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
	// versions >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
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
					&Batch{Type: cassandraprotocol.BatchType(42)},
					nil,
					errors.New("invalid BATCH type: 42"),
				},
				{
					"empty batch",
					NewBatch(),
					[]byte{cassandraprotocol.BatchTypeLogged},
					errors.New("BATCH messages must contain at least one child query"),
				},
				{
					"batch with 2 children",
					NewBatch(WithBatchChildren(
						NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4})),
						NewPreparedBatchChild([]byte{0xca, 0xfe, 0xba, 0xbe}, primitives.NewValue([]byte{5, 6, 7, 8})),
					)),
					[]byte{
						cassandraprotocol.BatchTypeLogged,
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 1, // consistency
						0, 0, 0, 0, // flags
					},
					nil,
				},
				{
					"batch with custom options",
					NewBatch(
						WithBatchType(cassandraprotocol.BatchTypeUnlogged),
						WithBatchChildren(NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4}))),
						WithBatchConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						WithBatchSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithBatchDefaultTimestamp(123),
						WithBatchKeyspace("ks1"),
						WithBatchNowInSeconds(234),
					),
					[]byte{
						cassandraprotocol.BatchTypeUnlogged,
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
	codec := &BatchCodec{}
	// versions <= 4
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersion4; version++ {
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
					NewBatch(),
					primitives.LengthOfByte +
						primitives.LengthOfShort + // children count
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte, // flags
					nil,
				},
				{
					"batch with 2 children",
					NewBatch(WithBatchChildren(
						NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4})),
						NewPreparedBatchChild([]byte{0xca, 0xfe, 0xba, 0xbe}, primitives.NewValue([]byte{5, 6, 7, 8})),
					)),
					primitives.LengthOfByte +
						primitives.LengthOfShort + // children count
						primitives.LengthOfByte + // child 1 kind
						primitives.LengthOfLongString("INSERT") + // child 1 query
						primitives.LengthOfShort + // child values count
						primitives.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitives.LengthOfByte + // child 2 kind
						primitives.LengthOfShortBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // child 1 query
						primitives.LengthOfShort + // child values count
						primitives.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 2 value 1
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte, // flags
					nil,
				},
				{
					"batch with custom options",
					NewBatch(
						WithBatchType(cassandraprotocol.BatchTypeUnlogged),
						WithBatchChildren(NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4}))),
						WithBatchConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						WithBatchSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithBatchDefaultTimestamp(123),
					),
					primitives.LengthOfByte +
						primitives.LengthOfShort + // children count
						primitives.LengthOfByte + // child 1 kind
						primitives.LengthOfLongString("INSERT") + // child 1 query
						primitives.LengthOfShort + // child values count
						primitives.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitives.LengthOfShort + // consistency
						primitives.LengthOfByte + // flags
						primitives.LengthOfShort + // serial consistency
						primitives.LengthOfLong, // default timestamp
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
	// versions >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
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
					NewBatch(),
					primitives.LengthOfByte +
						primitives.LengthOfShort + // children count
						primitives.LengthOfShort + // consistency
						primitives.LengthOfInt, // flags
					nil,
				},
				{
					"batch with 2 children",
					NewBatch(WithBatchChildren(
						NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4})),
						NewPreparedBatchChild([]byte{0xca, 0xfe, 0xba, 0xbe}, primitives.NewValue([]byte{5, 6, 7, 8})),
					)),
					primitives.LengthOfByte +
						primitives.LengthOfShort + // children count
						primitives.LengthOfByte + // child 1 kind
						primitives.LengthOfLongString("INSERT") + // child 1 query
						primitives.LengthOfShort + // child values count
						primitives.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitives.LengthOfByte + // child 2 kind
						primitives.LengthOfShortBytes([]byte{0xca, 0xfe, 0xba, 0xbe}) + // child 1 query
						primitives.LengthOfShort + // child values count
						primitives.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 2 value 1
						primitives.LengthOfShort + // consistency
						primitives.LengthOfInt, // flags
					nil,
				},
				{
					"batch with custom options",
					NewBatch(
						WithBatchType(cassandraprotocol.BatchTypeUnlogged),
						WithBatchChildren(NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4}))),
						WithBatchConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						WithBatchSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithBatchDefaultTimestamp(123),
						WithBatchKeyspace("ks1"),
						WithBatchNowInSeconds(234),
					),
					primitives.LengthOfByte +
						primitives.LengthOfShort + // children count
						primitives.LengthOfByte + // child 1 kind
						primitives.LengthOfLongString("INSERT") + // child 1 query
						primitives.LengthOfShort + // child values count
						primitives.LengthOfInt + len([]byte{1, 2, 3, 4}) + // child 1 value 1
						primitives.LengthOfShort + // consistency
						primitives.LengthOfInt + // flags
						primitives.LengthOfShort + // serial consistency
						primitives.LengthOfLong + // default timestamp
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

func TestBatchCodec_Decode(t *testing.T) {
	codec := &BatchCodec{}
	// versions <= 4
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersion4; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"invalid batch type",
					[]byte{
						42,   // bach type
						0, 0, // children count
						0, 1, // consistency
						0, // flags
					},
					nil,
					errors.New("invalid BATCH type: 42"),
				},
				{
					"empty batch",
					[]byte{
						cassandraprotocol.BatchTypeLogged,
						0, 0, // children count
						0, 1, // consistency
						0, // flags
					},
					&Batch{
						Children:          []*BatchChild{},
						Consistency:       cassandraprotocol.ConsistencyLevelOne,
						SerialConsistency: cassandraprotocol.ConsistencyLevelSerial,
						DefaultTimestamp:  DefaultTimestampNone,
						NowInSeconds:      NowInSecondsNone,
					},
					nil,
				},
				{
					"batch with 2 children",
					[]byte{
						cassandraprotocol.BatchTypeLogged,
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 1, // consistency
						0, // flags
					},
					NewBatch(WithBatchChildren(
						NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4})),
						NewPreparedBatchChild([]byte{0xca, 0xfe, 0xba, 0xbe}, primitives.NewValue([]byte{5, 6, 7, 8})),
					)),
					nil,
				},
				{
					"batch with custom options",
					[]byte{
						cassandraprotocol.BatchTypeUnlogged,
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
					NewBatch(
						WithBatchType(cassandraprotocol.BatchTypeUnlogged),
						WithBatchChildren(NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4}))),
						WithBatchConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						WithBatchSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithBatchDefaultTimestamp(123),
					),
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
	// versions >= 5
	for version := cassandraprotocol.ProtocolVersion5; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"invalid batch type",
					[]byte{
						42,   // bach type
						0, 0, // children count
						0, 1, // consistency
						0, 0, 0, 0, // flags
					},
					nil,
					errors.New("invalid BATCH type: 42"),
				},
				{
					"empty batch",
					[]byte{
						cassandraprotocol.BatchTypeLogged,
						0, 0, // children count
						0, 1, // consistency
						0, 0, 0, 0, // flags
					},
					&Batch{
						Children:          []*BatchChild{},
						Consistency:       cassandraprotocol.ConsistencyLevelOne,
						SerialConsistency: cassandraprotocol.ConsistencyLevelSerial,
						DefaultTimestamp:  DefaultTimestampNone,
						NowInSeconds:      NowInSecondsNone,
					},
					nil,
				},
				{
					"batch with 2 children",
					[]byte{
						cassandraprotocol.BatchTypeLogged,
						0, 2, // children count
						0,                            // child 1 kind
						0, 0, 0, 6, I, N, S, E, R, T, // child 1 query
						0, 1, // child 1 values count
						0, 0, 0, 4, 1, 2, 3, 4, // child 1 value 1
						1,                            // child 2 kind
						0, 4, 0xca, 0xfe, 0xba, 0xbe, // child 2 query id
						0, 1, // child 2 values count
						0, 0, 0, 4, 5, 6, 7, 8, // child 2 value 1
						0, 1, // consistency
						0, 0, 0, 0, // flags
					},
					NewBatch(WithBatchChildren(
						NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4})),
						NewPreparedBatchChild([]byte{0xca, 0xfe, 0xba, 0xbe}, primitives.NewValue([]byte{5, 6, 7, 8})),
					)),
					nil,
				},
				{
					"batch with custom options",
					[]byte{
						cassandraprotocol.BatchTypeUnlogged,
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
					NewBatch(
						WithBatchType(cassandraprotocol.BatchTypeUnlogged),
						WithBatchChildren(NewQueryBatchChild("INSERT", primitives.NewValue([]byte{1, 2, 3, 4}))),
						WithBatchConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalQuorum),
						WithBatchSerialConsistencyLevel(cassandraprotocol.ConsistencyLevelLocalSerial),
						WithBatchDefaultTimestamp(123),
						WithBatchKeyspace("ks1"),
						WithBatchNowInSeconds(234),
					),
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
