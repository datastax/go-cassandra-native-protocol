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

func TestRegister_DeepCopy(t *testing.T) {
	msg := &Register{
		EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange},
	}

	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)

	cloned.EventTypes = []primitive.EventType{primitive.EventTypeSchemaChange, primitive.EventTypeStatusChange}

	assert.NotEqual(t, msg, cloned)

	assert.Equal(t, []primitive.EventType{primitive.EventTypeSchemaChange}, msg.EventTypes)

	assert.Equal(t, []primitive.EventType{primitive.EventTypeSchemaChange, primitive.EventTypeStatusChange}, cloned.EventTypes)
}

func TestRegisterCodec_Encode(t *testing.T) {
	codec := &registerCodec{}
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"register all events",
					&Register{EventTypes: []primitive.EventType{
						primitive.EventTypeSchemaChange,
						primitive.EventTypeTopologyChange,
						primitive.EventTypeStatusChange,
					}},
					[]byte{
						0, 3, // list length
						// element SCHEMA_CHANGE
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						// element TOPOLOGY_CHANGE
						0, 15, T, O, P, O, L, O, G, Y, __, C, H, A, N, G, E,
						// element STATUS_CHANGE
						0, 13, S, T, A, T, U, S, __, C, H, A, N, G, E,
					},
					nil,
				},
				{
					"not a register",
					&Options{},
					nil,
					errors.New("expected *message.Register, got *message.Options"),
				},
				{
					"register with no events",
					&Register{},
					nil,
					errors.New("REGISTER messages must have at least one event type"),
				},
				{
					"register with wrong event",
					&Register{EventTypes: []primitive.EventType{"NOT A VALID EVENT"}},
					nil,
					errors.New("invalid event type: NOT A VALID EVENT"),
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

func TestRegisterCodec_EncodedLength(t *testing.T) {
	codec := &registerCodec{}
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"register all events",
					&Register{EventTypes: []primitive.EventType{
						primitive.EventTypeSchemaChange,
						primitive.EventTypeTopologyChange,
						primitive.EventTypeStatusChange,
					}},
					primitive.LengthOfShort + // list length
						primitive.LengthOfString("SCHEMA_CHANGE") +
						primitive.LengthOfString("TOPOLOGY_CHANGE") +
						primitive.LengthOfString("STATUS_CHANGE"),
					nil,
				},
				{
					"not a register",
					&Options{},
					-1,
					errors.New("expected *message.Register, got *message.Options"),
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

func TestRegisterCodec_Decode(t *testing.T) {
	codec := &registerCodec{}
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"register all events",
					[]byte{
						0, 3, // list length
						// element SCHEMA_CHANGE
						0, 13, S, C, H, E, M, A, __, C, H, A, N, G, E,
						// element TOPOLOGY_CHANGE
						0, 15, T, O, P, O, L, O, G, Y, __, C, H, A, N, G, E,
						// element STATUS_CHANGE
						0, 13, S, T, A, T, U, S, __, C, H, A, N, G, E,
					},
					&Register{EventTypes: []primitive.EventType{
						primitive.EventTypeSchemaChange,
						primitive.EventTypeTopologyChange,
						primitive.EventTypeStatusChange,
					}},
					nil,
				},
				{
					"register with no events", // not tolerated when encoding
					[]byte{0, 0},
					&Register{EventTypes: []primitive.EventType{}},
					nil,
				},
				{
					"register with wrong event",
					[]byte{
						0, 1, // list length
						0, 13, U, N, K, N, O, W, N, __, E, V, E, N, T,
					},
					nil,
					errors.New("invalid event type: UNKNOWN_EVENT"),
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
