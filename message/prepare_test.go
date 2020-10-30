package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrepareCodec_Encode(t *testing.T) {
	codec := &PrepareCodec{}
	// versions <= 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"prepare simple",
					&Prepare{"SELECT", ""},
					[]byte{
						0, 0, 0, 6, S, E, L, E, C, T,
					},
					nil,
				},
				{
					"not a prepare",
					&Ready{},
					nil,
					errors.New("expected *message.Prepare, got *message.Ready"),
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
					"prepare simple",
					&Prepare{"SELECT", ""},
					[]byte{
						0, 0, 0, 6, S, E, L, E, C, T,
						0, 0, 0, 0, // flags
					},
					nil,
				},
				{
					"prepare with keyspace",
					&Prepare{"SELECT", "ks"},
					[]byte{
						0, 0, 0, 6, S, E, L, E, C, T,
						0, 0, 0, 1, // flags
						0, 2, k, s, // keyspace
					},
					nil,
				},
				{
					"not a prepare",
					&Ready{},
					nil,
					errors.New("expected *message.Prepare, got *message.Ready"),
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

func TestPrepareCodec_EncodedLength(t *testing.T) {
	codec := &PrepareCodec{}
	// versions <= 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"prepare simple",
					&Prepare{"SELECT", ""},
					primitive.LengthOfLongString("SELECT"),
					nil,
				},
				{
					"not a prepare",
					&Ready{},
					-1,
					errors.New("expected *message.Prepare, got *message.Ready"),
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
					"prepare simple",
					&Prepare{"SELECT", ""},
					primitive.LengthOfLongString("SELECT") +
						primitive.LengthOfInt, // flags
					nil,
				},
				{
					"prepare with keyspace",
					&Prepare{"SELECT", "ks"},
					primitive.LengthOfLongString("SELECT") +
						primitive.LengthOfInt + // flags
						primitive.LengthOfString("ks"), // keyspace
					nil,
				},
				{
					"not a prepare",
					&Ready{},
					-1,
					errors.New("expected *message.Prepare, got *message.Ready"),
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

func TestPrepareCodec_Decode(t *testing.T) {
	codec := &PrepareCodec{}
	// versions <= 4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersionDse1} {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"prepare simple",
					[]byte{
						0, 0, 0, 6, S, E, L, E, C, T,
					},
					&Prepare{"SELECT", ""},
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
					"prepare simple",
					[]byte{
						0, 0, 0, 6, S, E, L, E, C, T,
						0, 0, 0, 0, // flags
					},
					&Prepare{"SELECT", ""},
					nil,
				},
				{
					"prepare with keyspace",
					[]byte{
						0, 0, 0, 6, S, E, L, E, C, T,
						0, 0, 0, 1, // flags
						0, 2, k, s, // keyspace
					},
					&Prepare{"SELECT", "ks"},
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
