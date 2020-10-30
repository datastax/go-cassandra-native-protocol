package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResultCodec_Encode_SchemaChange(test *testing.T) {
	codec := &ResultCodec{}
	// versions < 4
	for _, version := range primitives.AllProtocolVersionsLesserThan(primitives.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"schema change result keyspace",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					nil,
				},
				{
					"schema change result table",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					nil,
				},
				{
					"schema change result type",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					nil,
				},
				{
					"schema change result function",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
					},
					fmt.Errorf("FUNCTION schema change targets are not supported in protocol version %d", version),
				},
				{
					"schema change result aggregate",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
					},
					fmt.Errorf("AGGREGATE schema change targets are not supported in protocol version %d", version),
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
	for _, version := range primitives.AllProtocolVersionsGreaterThanOrEqualTo(primitives.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"schema change result keyspace",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					nil,
				},
				{
					"schema change result table",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					nil,
				},
				{
					"schema change result type",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					nil,
				},
				{
					"schema change result function",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 0, 0, 5, // result type
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
					"schema change result aggregate",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					[]byte{
						0, 0, 0, 5, // result type
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

func TestResultCodec_EncodedLength_SchemaChange(test *testing.T) {
	codec := &ResultCodec{}
	for _, version := range primitives.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"schema change result keyspace",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					primitives.LengthOfInt +
						primitives.LengthOfString(primitives.SchemaChangeTypeCreated) +
						primitives.LengthOfString(primitives.SchemaChangeTargetKeyspace) +
						primitives.LengthOfString("ks1"),
					nil,
				},
				{
					"schema change result table",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					primitives.LengthOfInt +
						primitives.LengthOfString(primitives.SchemaChangeTypeCreated) +
						primitives.LengthOfString(primitives.SchemaChangeTargetTable) +
						primitives.LengthOfString("ks1") +
						primitives.LengthOfString("table1"),
					nil,
				},
				{
					"schema change result type",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					primitives.LengthOfInt +
						primitives.LengthOfString(primitives.SchemaChangeTypeCreated) +
						primitives.LengthOfString(primitives.SchemaChangeTargetType) +
						primitives.LengthOfString("ks1") +
						primitives.LengthOfString("udt1"),
					nil,
				},
				{
					"schema change result function",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					primitives.LengthOfInt +
						primitives.LengthOfString(primitives.SchemaChangeTypeCreated) +
						primitives.LengthOfString(primitives.SchemaChangeTargetFunction) +
						primitives.LengthOfString("ks1") +
						primitives.LengthOfString("func1") +
						primitives.LengthOfStringList([]string{"int", "varchar"}),
					nil,
				},
				{
					"schema change result aggregate",
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					primitives.LengthOfInt +
						primitives.LengthOfString(primitives.SchemaChangeTypeCreated) +
						primitives.LengthOfString(primitives.SchemaChangeTargetAggregate) +
						primitives.LengthOfString("ks1") +
						primitives.LengthOfString("agg1") +
						primitives.LengthOfStringList([]string{"int", "varchar"}),
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

func TestResultCodec_Decode_SchemaChange(test *testing.T) {
	codec := &ResultCodec{}
	// versions < 4
	for _, version := range primitives.AllProtocolVersionsLesserThan(primitives.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"schema change result keyspace",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					nil,
				},
				{
					"schema change result table",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					nil,
				},
				{
					"schema change result type",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					nil,
				},
				{
					"schema change result function",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
					},
					nil,
					fmt.Errorf("FUNCTION schema change targets are not supported in protocol version %d", version),
				},
				{
					"schema change result aggregate",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
					},
					nil,
					fmt.Errorf("AGGREGATE schema change targets are not supported in protocol version %d", version),
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
	for _, version := range primitives.AllProtocolVersionsGreaterThanOrEqualTo(primitives.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"schema change result keyspace",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, K, E, Y, S, P, A, C, E,
						0, 3, k, s, _1,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					nil,
				},
				{
					"schema change result table",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 5, T, A, B, L, E,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					nil,
				},
				{
					"schema change result type",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 4, T, Y, P, E,
						0, 3, k, s, _1,
						0, 4, u, d, t, _1,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					nil,
				},
				{
					"schema change result function",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 8, F, U, N, C, T, I, O, N,
						0, 3, k, s, _1,
						0, 5, f, u, n, c, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					nil,
				},
				{
					"schema change result aggregate",
					[]byte{
						0, 0, 0, 5, // result type
						0, 7, C, R, E, A, T, E, D,
						0, 9, A, G, G, R, E, G, A, T, E,
						0, 3, k, s, _1,
						0, 4, a, g, g, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					&SchemaChangeResult{
						ChangeType: primitives.SchemaChangeTypeCreated,
						Target:     primitives.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
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
