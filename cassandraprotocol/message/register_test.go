package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegisterCodec_Encode(t *testing.T) {
	codec := &RegisterCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
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
	codec := &RegisterCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
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
	codec := &RegisterCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
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
