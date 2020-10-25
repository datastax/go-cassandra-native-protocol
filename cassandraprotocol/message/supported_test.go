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

func TestSupportedCodec_Encode(test *testing.T) {
	codec := &SupportedCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected [][]byte // required because there can be multiple valid encodings
				err      error
			}{
				{
					"supported with nil options",
					&Supported{},
					[][]byte{{0, 0}},
					nil,
				},
				{
					"supported with empty options",
					&Supported{Options: map[string][]string{}},
					[][]byte{{0, 0}},
					nil,
				},
				{
					"supported with 1 option",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}}},
					[][]byte{{
						0, 1, // map length
						// key "option1"
						0, 7, o, p, t, i, o, n, _1,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _1, a,
						// value1b
						0, 7, v, a, l, u, e, _1, b,
					}},
					nil,
				},
				{
					"supported with 2 options",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}, "option2": {"value2a", "value2b"}}},
					// we have two possible encodings because maps do not have deterministic iteration order
					[][]byte{
						{
							0, 2, // map length
							// key "option1"
							0, 7, o, p, t, i, o, n, _1,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _1, a,
							// value1b
							0, 7, v, a, l, u, e, _1, b,
							// key "option2"
							0, 7, o, p, t, i, o, n, _2,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _2, a,
							// value1b
							0, 7, v, a, l, u, e, _2, b,
						},
						{
							0, 2, // map length
							// key "option2"
							0, 7, o, p, t, i, o, n, _2,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _2, a,
							// value1b
							0, 7, v, a, l, u, e, _2, b,
							// key "option1"
							0, 7, o, p, t, i, o, n, _1,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _1, a,
							// value1b
							0, 7, v, a, l, u, e, _1, b,
						},
					},
					nil,
				},
				{
					"not a supported",
					&Options{},
					nil,
					errors.New("expected *message.Supported, got *message.Options"),
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
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

func TestSupportedCodec_EncodedLength(test *testing.T) {
	codec := &SupportedCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"supported with nil options",
					&Supported{},
					primitives.LengthOfShort, // map length
					nil,
				},
				{
					"supported with empty options",
					&Supported{Options: map[string][]string{}},
					primitives.LengthOfShort, // map length
					nil,
				},
				{
					"supported with 1 option",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}}},
					primitives.LengthOfShort + // map length
						primitives.LengthOfString("option1") + // map key
						primitives.LengthOfShort + // list length
						primitives.LengthOfString("value1a") + // map value
						primitives.LengthOfString("value1b"), // map value
					nil,
				},
				{
					"supported with 2 options",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}, "option2": {"value2a", "value2b"}}},
					primitives.LengthOfShort + // map length
						primitives.LengthOfString("option1") + // map key
						primitives.LengthOfShort + // list length
						primitives.LengthOfString("value1a") + // map value
						primitives.LengthOfString("value1b") + // map value
						primitives.LengthOfString("option2") + // map key
						primitives.LengthOfShort + // list length
						primitives.LengthOfString("value2a") + // map value
						primitives.LengthOfString("value2b"), // map value
					nil,
				},
				{
					"not a supported",
					&Options{},
					-1,
					errors.New("expected *message.Supported, got *message.Options"),
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

func TestSupportedCodec_Decode(test *testing.T) {
	codec := &SupportedCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"supported with empty options",
					[]byte{0, 0},
					&Supported{Options: map[string][]string{}},
					nil,
				},
				{
					"supported with 1 option",
					[]byte{
						0, 1, // map length
						// key "option1"
						0, 7, o, p, t, i, o, n, _1,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _1, a,
						// value1b
						0, 7, v, a, l, u, e, _1, b,
					},
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}}},
					nil,
				},
				{
					"supported with 2 options",
					// we have two possible encodings because maps do not have deterministic iteration order
					[]byte{
						0, 2, // map length
						// key "option1"
						0, 7, o, p, t, i, o, n, _1,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _1, a,
						// value1b
						0, 7, v, a, l, u, e, _1, b,
						// key "option2"
						0, 7, o, p, t, i, o, n, _2,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _2, a,
						// value1b
						0, 7, v, a, l, u, e, _2, b,
					},
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}, "option2": {"value2a", "value2b"}}},
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
