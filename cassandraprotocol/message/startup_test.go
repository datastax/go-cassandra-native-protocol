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

func TestStartupCodec_Encode(t *testing.T) {
	codec := &StartupCodec{}
	tests := []struct {
		name     string
		input    Message
		expected [][]byte // required because there can be multiple valid encodings
		err      error
	}{
		{
			"startup with default options",
			NewStartup(),
			[][]byte{{
				0, 1, // map length
				// key "CQL_VERSION"
				0, 11, C, Q, L, __, V, E, R, S, I, O, N,
				// value "3.0.0"
				0, 5, _3, dot, _0, dot, _0,
			}},
			nil,
		},
		{
			"startup with nil options",
			&Startup{},
			[][]byte{{0, 0}},
			nil,
		},
		{
			"startup with compression",
			NewStartup(WithCompression("LZ4")),
			[][]byte{
				{
					0, 2,
					// key "CQL_VERSION"
					0, 11, C, Q, L, __, V, E, R, S, I, O, N,
					// value "3.0.0"
					0, 5, _3, dot, _0, dot, _0,
					// key "COMPRESSION"
					0, 11, C, O, M, P, R, E, S, S, I, O, N,
					// value "LZ4"
					0, 3, L, Z, _4,
				},
				{
					0, 2,
					// key "COMPRESSION"
					0, 11, C, O, M, P, R, E, S, S, I, O, N,
					// value "LZ4"
					0, 3, L, Z, _4,
					// key "CQL_VERSION"
					0, 11, C, Q, L, __, V, E, R, S, I, O, N,
					// value "3.0.0"
					0, 5, _3, dot, _0, dot, _0,
				},
			},
			nil,
		},
		{
			"startup with custom options",
			NewStartup(WithOptions(map[string]string{"CQL_VERSION": "3.4.5", "COMPRESSION": "SNAPPY"})),
			// we have two possible encodings because maps do not have deterministic iteration order
			[][]byte{
				{
					0, 2, // map length
					// key "CQL_VERSION"
					0, 11, C, Q, L, __, V, E, R, S, I, O, N,
					// value "3.4.5"
					0, 5, _3, dot, _4, dot, _5,
					// key "COMPRESSION"
					0, 11, C, O, M, P, R, E, S, S, I, O, N,
					// value "SNAPPY"
					0, 6, S, N, A, P, P, Y,
				},
				{
					0, 2, // map length
					// key "COMPRESSION"
					0, 11, C, O, M, P, R, E, S, S, I, O, N,
					// value "SNAPPY"
					0, 6, S, N, A, P, P, Y,
					// key "CQL_VERSION"
					0, 11, C, Q, L, __, V, E, R, S, I, O, N,
					// value "3.4.5"
					0, 5, _3, dot, _4, dot, _5,
				},
			},
			nil,
		},
		{
			"not a startup",
			&Options{},
			nil,
			errors.New("expected *message.Startup, got *message.Options"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					if err == nil {
						assert.Contains(t, tt.expected, dest.Bytes())
						assert.Nil(t, tt.err)
					} else {
						assert.Equal(t, tt.err, err)
					}
				})
			}
		})
	}
}

func TestStartupCodec_EncodedLength(t *testing.T) {
	codec := &StartupCodec{}
	tests := []struct {
		name     string
		input    Message
		expected int
		err      error
	}{
		{
			"startup with default options",
			NewStartup(),
			primitives.LengthOfShort + // map length
				primitives.LengthOfString("CQL_VERSION") + // map key
				primitives.LengthOfString("3.0.0"), // map value
			nil,
		},
		{
			"startup with nil options",
			&Startup{},
			primitives.LengthOfShort, // map length
			nil,
		},
		{
			"startup with compression",
			NewStartup(WithCompression("LZ4")),
			primitives.LengthOfShort + // map length
				primitives.LengthOfString("CQL_VERSION") + // map key
				primitives.LengthOfString("3.0.0") + // map value
				primitives.LengthOfString("COMPRESSION") + // map key
				primitives.LengthOfString("LZ4"), // map value
			nil,
		},
		{
			"startup with custom options",
			NewStartup(WithOptions(map[string]string{"CQL_VERSION": "3.4.5", "COMPRESSION": "SNAPPY"})),
			primitives.LengthOfShort + // map length
				primitives.LengthOfString("CQL_VERSION") + // map key
				primitives.LengthOfString("3.4.5") + // map value
				primitives.LengthOfString("COMPRESSION") + // map key
				primitives.LengthOfString("SNAPPY"), // map value
			nil,
		},
		{
			"not a startup",
			&Options{},
			-1,
			errors.New("expected *message.Startup, got *message.Options"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
				t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestStartupCodec_Decode(t *testing.T) {
	codec := &StartupCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"startup with default options",
					[]byte{
						0, 1, // map length
						// key "CQL_VERSION"
						0, 11, C, Q, L, __, V, E, R, S, I, O, N,
						// value "3.0.0"
						0, 5, _3, dot, _0, dot, _0,
					},
					NewStartup(),
					nil,
				},
				{
					"startup with empty options",
					[]byte{0, 0},
					&Startup{Options: map[string]string{}},
					nil,
				},
				{
					"startup with compression",
					[]byte{
						0, 2,
						// key "CQL_VERSION"
						0, 11, C, Q, L, __, V, E, R, S, I, O, N,
						// value "3.0.0"
						0, 5, _3, dot, _0, dot, _0,
						// key "COMPRESSION"
						0, 11, C, O, M, P, R, E, S, S, I, O, N,
						// value "LZ4"
						0, 3, L, Z, _4,
					},
					NewStartup(WithCompression("LZ4")),
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
