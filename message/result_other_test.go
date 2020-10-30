package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResultCodec_Encode_Other(test *testing.T) {
	codec := &resultCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"void result",
					&VoidResult{},
					[]byte{
						0, 0, 0, 1, // result type
					},
					nil,
				},
				{
					"set keyspace result",
					&SetKeyspaceResult{Keyspace: "ks1"},
					[]byte{
						0, 0, 0, 3, // result type
						0, 3, k, s, _1,
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

func TestResultCodec_EncodedLength_Other(test *testing.T) {
	codec := &resultCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"void result",
					&VoidResult{},
					primitive.LengthOfInt,
					nil,
				},
				{
					"set keyspace result",
					&SetKeyspaceResult{Keyspace: "ks1"},
					primitive.LengthOfInt + primitive.LengthOfString("ks1"),
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

func TestResultCodec_Decode_Other(test *testing.T) {
	codec := &resultCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"void result",
					[]byte{
						0, 0, 0, 1, // result type
					},
					&VoidResult{},
					nil,
				},
				{
					"set keyspace result",
					[]byte{
						0, 0, 0, 3, // result type
						0, 3, k, s, _1,
					},
					&SetKeyspaceResult{Keyspace: "ks1"},
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
