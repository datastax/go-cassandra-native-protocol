package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthenticateCodec_Encode(t *testing.T) {
	codec := &AuthenticateCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"simple authenticate",
					&Authenticate{"dummy"},
					[]byte{0, 5, d, u, m, m, y},
					nil,
				},
				{
					"not an authenticate",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					nil,
					errors.New("expected *message.Authenticate, got *message.AuthChallenge"),
				},
				{
					"authenticate nil authenticator",
					&Authenticate{""},
					nil,
					errors.New("AUTHENTICATE authenticator cannot be empty"),
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

func TestAuthenticateCodec_EncodedLength(t *testing.T) {
	codec := &AuthenticateCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"simple authenticate",
					&Authenticate{"dummy"},
					primitive.LengthOfString("dummy"),
					nil,
				},
				{
					"not an authenticate",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					-1,
					errors.New("expected *message.Authenticate, got *message.AuthChallenge"),
				},
				{
					"authenticate nil authenticator",
					&Authenticate{""},
					primitive.LengthOfString(""),
					nil,
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

func TestAuthenticateCodec_Decode(t *testing.T) {
	codec := &AuthenticateCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"simple authenticate",
					[]byte{0, 5, d, u, m, m, y},
					&Authenticate{"dummy"},
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
