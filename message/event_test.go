package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestEventCodec_Encode(test *testing.T) {
	codec := &eventCodec{}
	// versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"schema change event keyspace",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					nil,
				},
				{
					"schema change event table",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					nil,
				},
				{
					"schema change event type",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					nil,
				},
				{
					"schema change event function",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
					},
					fmt.Errorf("FUNCTION schema change targets are not supported in protocol version %d", version),
				},
				{
					"schema change event aggregate",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
					},
					fmt.Errorf("AGGREGATE schema change targets are not supported in protocol version %d", version),
				},
				{
					"status change event",
					&StatusChangeEvent{
						ChangeType: primitive.StatusChangeTypeUp,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					[]byte{
						0, 13, S, T, A, T, U, S, __, C, H, A, N, G, E,
						0, 2, U, P,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
					},
					nil,
				},
				{
					"topology change event",
					&TopologyChangeEvent{
						ChangeType: primitive.TopologyChangeTypeNewNode,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					[]byte{
						0, 15, T, O, P, O, L, O, G, Y, __, C, H, A, N, G, E,
						0, 8, N, E, W, __, N, O, D, E,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
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
	// versions >= 4
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"schema change event keyspace",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					nil,
				},
				{
					"schema change event table",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					nil,
				},
				{
					"schema change event type",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					nil,
				},
				{
					"schema change event function",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
						0, 5, f, u, n, c, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					nil,
				},
				{
					"schema change event aggregate",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
						0, 4, a, g, g, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					nil,
				},
				{
					"status change event",
					&StatusChangeEvent{
						ChangeType: primitive.StatusChangeTypeUp,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					[]byte{
						0, 13, S, T, A, T, U, S, __, C, H, A, N, G, E,
						0, 2, U, P,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
					},
					nil,
				},
				{
					"topology change event",
					&TopologyChangeEvent{
						ChangeType: primitive.TopologyChangeTypeNewNode,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					[]byte{
						0, 15, T, O, P, O, L, O, G, Y, __, C, H, A, N, G, E,
						0, 8, N, E, W, __, N, O, D, E,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
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

func TestEventCodec_EncodedLength(test *testing.T) {
	codec := &eventCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"schema change event keyspace",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					primitive.LengthOfString(primitive.EventTypeSchemaChange) +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetKeyspace) +
						primitive.LengthOfString("ks1"),
					nil,
				},
				{
					"schema change event table",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					primitive.LengthOfString(primitive.EventTypeSchemaChange) +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetTable) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1"),
					nil,
				},
				{
					"schema change event type",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					primitive.LengthOfString(primitive.EventTypeSchemaChange) +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetType) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("udt1"),
					nil,
				},
				{
					"schema change event function",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					primitive.LengthOfString(primitive.EventTypeSchemaChange) +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetFunction) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("func1") +
						primitive.LengthOfStringList([]string{"int", "varchar"}),
					nil,
				},
				{
					"schema change event aggregate",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					primitive.LengthOfString(primitive.EventTypeSchemaChange) +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetAggregate) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("agg1") +
						primitive.LengthOfStringList([]string{"int", "varchar"}),
					nil,
				},
				{
					"status change event",
					&StatusChangeEvent{
						ChangeType: primitive.StatusChangeTypeUp,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					primitive.LengthOfString(primitive.EventTypeStatusChange) +
						primitive.LengthOfString(primitive.StatusChangeTypeUp) +
						primitive.LengthOfByte + net.IPv4len +
						primitive.LengthOfInt,
					nil,
				},
				{
					"topology change event",
					&TopologyChangeEvent{
						ChangeType: primitive.TopologyChangeTypeNewNode,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					primitive.LengthOfString(primitive.EventTypeTopologyChange) +
						primitive.LengthOfString(primitive.TopologyChangeTypeNewNode) +
						primitive.LengthOfByte + net.IPv4len +
						primitive.LengthOfInt,
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

func TestEventCodec_Decode(test *testing.T) {
	codec := &eventCodec{}
	// versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"schema change event keyspace",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					nil,
				},
				{
					"schema change event table",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					nil,
				},
				{
					"schema change event type",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					nil,
				},
				{
					"schema change event function",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
					},
					nil,
					fmt.Errorf("FUNCTION schema change targets are not supported in protocol version %d", version),
				},
				{
					"schema change event aggregate",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
					},
					nil,
					fmt.Errorf("AGGREGATE schema change targets are not supported in protocol version %d", version),
				},
				{
					"status change event",
					[]byte{
						0, 13, S, T, A, T, U, S, __, C, H, A, N, G, E,
						0, 2, U, P,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
					},
					&StatusChangeEvent{
						ChangeType: primitive.StatusChangeTypeUp,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					nil,
				},
				{
					"topology change event",
					[]byte{
						0, 15, T, O, P, O, L, O, G, Y, __, C, H, A, N, G, E,
						0, 8, N, E, W, __, N, O, D, E,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
					},
					&TopologyChangeEvent{
						ChangeType: primitive.TopologyChangeTypeNewNode,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
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
	// versions >= 4
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"schema change event keyspace",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					nil,
				},
				{
					"schema change event table",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					nil,
				},
				{
					"schema change event type",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					nil,
				},
				{
					"schema change event function",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
						0, 5, f, u, n, c, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					nil,
				},
				{
					"schema change event aggregate",
					[]byte{
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
						0, 4, a, g, g, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					nil,
				},
				{
					"status change event",
					[]byte{
						0, 13, S, T, A, T, U, S, __, C, H, A, N, G, E,
						0, 2, U, P,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
					},
					&StatusChangeEvent{
						ChangeType: primitive.StatusChangeTypeUp,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
						},
					},
					nil,
				},
				{
					"topology change event",
					[]byte{
						0, 15, T, O, P, O, L, O, G, Y, __, C, H, A, N, G, E,
						0, 8, N, E, W, __, N, O, D, E,
						4, 192, 168, 1, 1,
						0, 0, 0x23, 0x52,
					},
					&TopologyChangeEvent{
						ChangeType: primitive.TopologyChangeTypeNewNode,
						Address: &primitive.Inet{
							Addr: net.IPv4(192, 168, 1, 1),
							Port: 9042,
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
