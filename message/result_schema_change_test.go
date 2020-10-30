package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResultCodec_Encode_SchemaChange(test *testing.T) {
	codec := &ResultCodec{}
	// versions < 4
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"schema change result keyspace",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
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
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"schema change result keyspace",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
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
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"schema change result keyspace",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					primitive.LengthOfInt +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetKeyspace) +
						primitive.LengthOfString("ks1"),
					nil,
				},
				{
					"schema change result table",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
						Keyspace:   "ks1",
						Object:     "table1",
					},
					primitive.LengthOfInt +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetTable) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("table1"),
					nil,
				},
				{
					"schema change result type",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
						Keyspace:   "ks1",
						Object:     "udt1",
					},
					primitive.LengthOfInt +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetType) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("udt1"),
					nil,
				},
				{
					"schema change result function",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
						Keyspace:   "ks1",
						Object:     "func1",
						Arguments:  []string{"int", "varchar"},
					},
					primitive.LengthOfInt +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetFunction) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("func1") +
						primitive.LengthOfStringList([]string{"int", "varchar"}),
					nil,
				},
				{
					"schema change result aggregate",
					&SchemaChangeResult{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
						Keyspace:   "ks1",
						Object:     "agg1",
						Arguments:  []string{"int", "varchar"},
					},
					primitive.LengthOfInt +
						primitive.LengthOfString(primitive.SchemaChangeTypeCreated) +
						primitive.LengthOfString(primitive.SchemaChangeTargetAggregate) +
						primitive.LengthOfString("ks1") +
						primitive.LengthOfString("agg1") +
						primitive.LengthOfStringList([]string{"int", "varchar"}),
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
	for _, version := range primitive.AllProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
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
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetTable,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetType,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetFunction,
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
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetAggregate,
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
