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
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestSchemaChangeEvent_Clone(t *testing.T) {
	msg := &SchemaChangeEvent{
		ChangeType: primitive.SchemaChangeTypeCreated,
		Target:     primitive.SchemaChangeTargetAggregate,
		Keyspace:   "ks1",
		Object:     "aggregate",
		Arguments:  []string{"arg1"},
	}

	cloned := msg.Clone().(*SchemaChangeEvent)
	assert.Equal(t, msg, cloned)

	cloned.ChangeType = primitive.SchemaChangeTypeDropped
	cloned.Target = primitive.SchemaChangeTargetFunction
	cloned.Keyspace = "ks2"
	cloned.Object = "function"
	cloned.Arguments = []string{"arg2"}

	assert.Equal(t, primitive.SchemaChangeTypeCreated, msg.ChangeType)
	assert.Equal(t, primitive.SchemaChangeTargetAggregate, msg.Target)
	assert.Equal(t, "ks1", msg.Keyspace)
	assert.Equal(t, "aggregate", msg.Object)
	assert.Equal(t, []string{"arg1"}, msg.Arguments)

	assert.Equal(t, primitive.SchemaChangeTypeDropped, cloned.ChangeType)
	assert.Equal(t, primitive.SchemaChangeTargetFunction, cloned.Target)
	assert.Equal(t, "ks2", cloned.Keyspace)
	assert.Equal(t, "function", cloned.Object)
	assert.Equal(t, []string{"arg2"}, cloned.Arguments)
}

func TestStatusChangeEvent_Clone(t *testing.T) {
	msg := &StatusChangeEvent{
		ChangeType: primitive.StatusChangeTypeDown,
		Address:    &primitive.Inet{
			Addr: net.IP{0x01},
			Port: 80,
		},
	}

	cloned := msg.Clone().(*StatusChangeEvent)
	assert.Equal(t, msg, cloned)

	cloned.ChangeType = primitive.StatusChangeTypeUp
	cloned.Address = &primitive.Inet{
		Addr: net.IP{0x02},
		Port: 801,
	}

	assert.Equal(t, primitive.StatusChangeTypeDown, msg.ChangeType)
	assert.Equal(t, net.IP{0x01}, msg.Address.Addr)
	assert.EqualValues(t, 80, msg.Address.Port)

	assert.Equal(t, primitive.StatusChangeTypeUp, cloned.ChangeType)
	assert.Equal(t, net.IP{0x02}, cloned.Address.Addr)
	assert.EqualValues(t, 801, cloned.Address.Port)
}

func TestTopologyChangeEvent_Clone(t *testing.T) {
	msg := &TopologyChangeEvent{
		ChangeType: primitive.TopologyChangeTypeNewNode,
		Address:    &primitive.Inet{
			Addr: net.IP{0x01},
			Port: 80,
		},
	}

	cloned := msg.Clone().(*TopologyChangeEvent)
	assert.Equal(t, msg, cloned)

	cloned.ChangeType = primitive.TopologyChangeTypeRemovedNode
	cloned.Address = &primitive.Inet{
		Addr: net.IP{0x02},
		Port: 801,
	}

	assert.Equal(t, primitive.TopologyChangeTypeNewNode, msg.ChangeType)
	assert.Equal(t, net.IP{0x01}, msg.Address.Addr)
	assert.EqualValues(t, 80, msg.Address.Port)

	assert.Equal(t, primitive.TopologyChangeTypeRemovedNode, cloned.ChangeType)
	assert.Equal(t, net.IP{0x02}, cloned.Address.Addr)
	assert.EqualValues(t, 801, cloned.Address.Port)
}

func TestEventCodec_Encode(test *testing.T) {
	codec := &eventCodec{}
	// version = 2
	test.Run(primitive.ProtocolVersion2.String(), func(test *testing.T) {
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
					0, 3, k, s, _1,
					0, 0,
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
					0, 3, k, s, _1,
					0, 6, t, a, b, l, e, _1,
				},
				nil,
			},
			{
				"schema change result type",
				&SchemaChangeEvent{
					ChangeType: primitive.SchemaChangeTypeCreated,
					Target:     primitive.SchemaChangeTargetType,
					Keyspace:   "ks1",
					Object:     "udt1",
				},
				[]byte{
					0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
					0, 7, C, R, E, A, T, E, D,
				},
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion2, primitive.SchemaChangeTargetType),
			},
			{
				"schema change result function",
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
				},
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion2, primitive.SchemaChangeTargetFunction),
			},
			{
				"schema change result aggregate",
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
				},
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion2, primitive.SchemaChangeTargetAggregate),
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
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion2)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// version = 3
	test.Run(primitive.ProtocolVersion3.String(), func(test *testing.T) {
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
				},
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion3, primitive.SchemaChangeTargetFunction),
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
				},
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion3, primitive.SchemaChangeTargetAggregate),
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
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion3)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// versions >= 4
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(version.String(), func(test *testing.T) {
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
	// version = 2
	test.Run(primitive.ProtocolVersion2.String(), func(test *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"schema change event keyspace",
				&SchemaChangeEvent{
					ChangeType: primitive.SchemaChangeTypeCreated,
					Target:     primitive.SchemaChangeTargetKeyspace,
					Keyspace:   "ks1",
				},
				primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
					primitive.LengthOfString("ks1") +
					primitive.LengthOfString(""),
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
				primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
					primitive.LengthOfString("ks1") +
					primitive.LengthOfString("table1"),
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
				primitive.LengthOfString(string(primitive.EventTypeStatusChange)) +
					primitive.LengthOfString(string(primitive.StatusChangeTypeUp)) +
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
				primitive.LengthOfString(string(primitive.EventTypeTopologyChange)) +
					primitive.LengthOfString(string(primitive.TopologyChangeTypeNewNode)) +
					primitive.LengthOfByte + net.IPv4len +
					primitive.LengthOfInt,
				nil,
			},
		}
		for _, tt := range tests {
			test.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// version = 3
	test.Run(primitive.ProtocolVersion3.String(), func(test *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"schema change event keyspace",
				&SchemaChangeEvent{
					ChangeType: primitive.SchemaChangeTypeCreated,
					Target:     primitive.SchemaChangeTargetKeyspace,
					Keyspace:   "ks1",
				},
				primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTargetKeyspace)) +
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
				primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTargetTable)) +
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
				primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
					primitive.LengthOfString(string(primitive.SchemaChangeTargetType)) +
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
				-1,
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion3, primitive.SchemaChangeTargetFunction),
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
				-1,
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion3, primitive.SchemaChangeTargetAggregate),
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
				primitive.LengthOfString(string(primitive.EventTypeStatusChange)) +
					primitive.LengthOfString(string(primitive.StatusChangeTypeUp)) +
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
				primitive.LengthOfString(string(primitive.EventTypeTopologyChange)) +
					primitive.LengthOfString(string(primitive.TopologyChangeTypeNewNode)) +
					primitive.LengthOfByte + net.IPv4len +
					primitive.LengthOfInt,
				nil,
			},
		}
		for _, tt := range tests {
			test.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion3)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// versions >= 4
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"schema change event keyspace",
					&SchemaChangeEvent{
						ChangeType: primitive.SchemaChangeTypeCreated,
						Target:     primitive.SchemaChangeTargetKeyspace,
						Keyspace:   "ks1",
					},
					primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTargetKeyspace)) +
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
					primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTargetTable)) +
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
					primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTargetType)) +
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
					primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTargetFunction)) +
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
					primitive.LengthOfString(string(primitive.EventTypeSchemaChange)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTypeCreated)) +
						primitive.LengthOfString(string(primitive.SchemaChangeTargetAggregate)) +
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
					primitive.LengthOfString(string(primitive.EventTypeStatusChange)) +
						primitive.LengthOfString(string(primitive.StatusChangeTypeUp)) +
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
					primitive.LengthOfString(string(primitive.EventTypeTopologyChange)) +
						primitive.LengthOfString(string(primitive.TopologyChangeTypeNewNode)) +
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
	// version = 2
	test.Run(primitive.ProtocolVersion2.String(), func(test *testing.T) {
		tests := []decodeTestCase{
			{
				"schema change event keyspace",
				[]byte{
					0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
					0, 7, C, R, E, A, T, E, D,
					0, 3, k, s, _1,
					0, 0,
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
				actual, err := codec.Decode(source, primitive.ProtocolVersion2)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// version = 3
	test.Run(primitive.ProtocolVersion3.String(), func(test *testing.T) {
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
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion3, primitive.SchemaChangeTargetFunction),
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
				fmt.Errorf("invalid schema change target for %v: %v", primitive.ProtocolVersion3, primitive.SchemaChangeTargetAggregate),
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
				actual, err := codec.Decode(source, primitive.ProtocolVersion3)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	// versions >= 4
	for _, version := range primitive.AllProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(version.String(), func(test *testing.T) {
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
